package driver

import (
	"database/sql/driver"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"
)

// Exact-decimal bind funnel (issue #40, plan §P1-F).
//
// database/sql's restricted driver.Value union is int64 / float64 / bool /
// []byte / string / time.Time / nil. A high-precision DECIMAL/NUMERIC value
// has no lossless slot there: float64 caps at ~15-17 significant digits, and
// the only precision-preserving path into a decimal column is a *string*
// bind -- it ships as VARCHAR and the server casts the text straight to the
// column (no binary-float round-trip). See bindArgsToPreparedParams and
// issue #12.
//
// The remaining gap was the driver BOUNDARY: checkNamedValue rejected every
// custom type, so a caller holding a math/big value or a Stringer-based
// decimal could not reach that string path at all -- database/sql's default
// converter rejected the value before the driver saw it. This funnel admits
// the exact-decimal types JT400 accepts (a Number/decimal/String analogue)
// and renders each to a canonical plain-decimal string IN checkNamedValue,
// substituting that string back into the bind so it rides the existing
// precision-preserving VARCHAR path on cache-miss and the native
// packed/zoned-BCD path (toDecimalString -> encodePackedBCD/encodeZonedBCD,
// from the *PGM-stored parameter-marker format) on cache-hit. The conversion
// lives before the cache-miss/hit fork, so both paths see byte-identical
// input.
//
// This is type ADMISSION, not a new wire shape: a decimal type that already
// implements driver.Valuer (e.g. shopspring/decimal, whose Value() returns
// its String()) keeps flowing through database/sql's default Valuer path
// untouched -- canonicalDecimalString deliberately declines those so it
// never preempts an established Valuer contract. The native packed/zoned BCD
// *reshape* on cache-miss (for JT400 byte-equality) is a separate, deferred
// item; this change does not alter any wire byte the driver emits today.

// canonicalDecimalString reports whether v is an exact-decimal type the bind
// funnel should admit, and if so returns its canonical plain-decimal string
// form (the shape the string-bind path and toDecimalString both consume
// verbatim).
//
//   - ok==false, err==nil: v is not a type this funnel handles; the caller
//     should fall back to database/sql's default converter (driver.ErrSkip).
//   - ok==true, err==nil:  s is the canonical decimal string to bind.
//   - err!=nil:            v is an admitted type but cannot be rendered as an
//     exact finite decimal (e.g. a non-terminating *big.Rat, or a Stringer
//     whose text is not a decimal literal); surface the error to the caller.
//
// Precedence is deliberate. The concrete math/big types are matched first
// because none implements driver.Valuer and *big.Rat's String() ("3/2") is a
// fraction, not a decimal literal -- it must be detected structurally, never
// via the generic Stringer arm. driver.Valuer is then declined so existing
// Valuer-based decimals are left to the default path. fmt.Stringer is the
// final catch-all for decimal types that expose only String().
func canonicalDecimalString(v any) (string, bool, error) {
	switch x := v.(type) {
	case *big.Rat:
		s, err := ratToDecimalString(x)
		if err != nil {
			return "", false, err
		}
		return s, true, nil
	case *big.Int:
		// Integers are always exact and always a valid decimal literal.
		return x.String(), true, nil
	case *big.Float:
		// A finite *big.Float is a binary fraction, so it always has an
		// exact finite decimal expansion; route through *big.Rat to render
		// every digit (Text('f', -1) would emit the shortest round-tripping
		// form, not the exact value). Non-finite (Inf) has no decimal form.
		if x.IsInf() {
			return "", false, fmt.Errorf("cannot bind non-finite *big.Float as DECIMAL")
		}
		r := new(big.Rat)
		x.Rat(r)
		s, err := ratToDecimalString(r)
		if err != nil {
			return "", false, err
		}
		return s, true, nil
	}

	// The generic Stringer arm fills ONLY the gap database/sql's default
	// converter leaves: types it would reject outright. Decline everything
	// it already handles -- driver.Valuer (so an established Value() is never
	// preempted), the valid driver.Value kinds (crucially time.Time, which is
	// a Stringer but must stay a timestamp bind), and any basic reflect Kind
	// (so a Stringer enum over an int stays an int, not its label). Without
	// this guard the arm below would hijack time.Time and stringer-typed
	// scalars and error them as "not a decimal literal".
	if defaultConverterHandles(v) {
		return "", false, nil
	}
	if st, ok := v.(fmt.Stringer); ok {
		s, err := normalizeDecimalString(st.String())
		if err != nil {
			return "", false, fmt.Errorf("bind %T as DECIMAL: %w", v, err)
		}
		return s, true, nil
	}
	return "", false, nil
}

// defaultConverterHandles reports whether database/sql's
// driver.DefaultParameterConverter would accept v on its own -- i.e. v is a
// driver.Valuer, one of the seven valid driver.Value kinds (including
// time.Time), or a pointer/scalar whose reflect Kind the default converter
// maps (Int*/Uint*/Float*/Bool/String, or a byte slice). canonicalDecimalString
// uses it as the gate on the generic Stringer arm so the funnel only rescues
// values the default path would otherwise reject, never re-routing ones it
// already binds correctly. Mirrors the type/Kind logic in
// database/sql/driver.DefaultParameterConverter.ConvertValue.
func defaultConverterHandles(v any) bool {
	if _, ok := v.(driver.Valuer); ok {
		return true
	}
	switch v.(type) {
	case nil, bool, []byte, string, float64, int64, time.Time:
		return true
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return true // nil pointer -> SQL NULL; default converter handles it
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Bool, reflect.String:
		return true
	case reflect.Slice:
		return rv.Type().Elem().Kind() == reflect.Uint8 // []byte-shaped
	}
	return false
}

// ratToDecimalString renders r as an exact finite decimal string, or returns
// an error if r has no terminating decimal expansion. A reduced fraction
// terminates in base 10 iff its denominator's only prime factors are 2 and 5;
// the number of fractional digits is then max(v2, v5) where v2/v5 are the
// multiplicities of 2 and 5 in the denominator. At that scale FloatString is
// exact (it has nothing left to round), so the result loses no precision.
//
// The exact-or-error contract is deliberate (issue #40): a non-terminating
// rational like 1/3 cannot be represented in a fixed-scale DECIMAL column
// without silently choosing a rounding, so the driver refuses rather than
// guess. Callers wanting a rounded value should bind a decimal string or a
// decimal.Decimal at their chosen scale.
func ratToDecimalString(r *big.Rat) (string, error) {
	if r.IsInt() {
		return r.Num().String(), nil
	}
	denom := new(big.Int).Set(r.Denom())
	one := big.NewInt(1)
	two := big.NewInt(2)
	five := big.NewInt(5)
	rem := new(big.Int)

	twos := 0
	for {
		rem.Mod(denom, two)
		if rem.Sign() != 0 {
			break
		}
		denom.Quo(denom, two)
		twos++
	}
	fives := 0
	for {
		rem.Mod(denom, five)
		if rem.Sign() != 0 {
			break
		}
		denom.Quo(denom, five)
		fives++
	}
	if denom.Cmp(one) != 0 {
		return "", fmt.Errorf("*big.Rat %s has no terminating decimal expansion "+
			"(bind a decimal string or decimal.Decimal at the desired scale)", r.RatString())
	}
	scale := twos
	if fives > scale {
		scale = fives
	}
	return r.FloatString(scale), nil
}

// normalizeDecimalString validates a Stringer's text as a decimal literal and
// returns its canonical plain (non-scientific) form. Plain decimals are kept
// verbatim (only a leading '+' is trimmed) so the caller's exact text and
// scale survive; scientific-notation forms (e.g. "1.5E+5") are expanded to
// plain digits, mirroring JT400's SQLDataFactory.convertScientificNotation
// step before it builds the column BigDecimal. Fraction forms ("3/2") and any
// non-decimal text are rejected -- the packed/zoned BCD encoders and the
// server cast both require plain digits, and silently shipping an unparseable
// string would miscast on the server.
func normalizeDecimalString(s string) (string, error) {
	t := strings.TrimSpace(s)
	if t == "" {
		return "", fmt.Errorf("empty decimal string")
	}
	if strings.ContainsAny(t, "eE") {
		// Expand the exponent exactly. A decimal mantissa times a power of
		// ten always terminates, so ratToDecimalString never errors here;
		// the parse is what validates the literal.
		r, ok := new(big.Rat).SetString(t)
		if !ok || strings.Contains(t, "/") {
			return "", fmt.Errorf("%q is not a decimal literal", s)
		}
		return ratToDecimalString(r)
	}
	if !isPlainDecimal(t) {
		return "", fmt.Errorf("%q is not a decimal literal", s)
	}
	if t[0] == '+' {
		t = t[1:]
	}
	return t, nil
}

// isPlainDecimal reports whether s is an optionally-signed fixed-point decimal
// literal: [+-]? DIGIT* ('.' DIGIT*)? with at least one digit overall and at
// most one '.'. It intentionally rejects exponents, fraction slashes,
// grouping separators, and whitespace (callers handle those before calling).
func isPlainDecimal(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	if s[0] == '+' || s[0] == '-' {
		i++
	}
	digits, dots := 0, 0
	for ; i < len(s); i++ {
		switch c := s[i]; {
		case c >= '0' && c <= '9':
			digits++
		case c == '.':
			dots++
			if dots > 1 {
				return false
			}
		default:
			return false
		}
	}
	return digits > 0
}
