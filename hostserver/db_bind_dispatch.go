package hostserver

// db_bind_dispatch.go holds the unified, PREPARE_DESCRIBE/PMF-driven bind-
// shape dispatcher for issue #40 -- the Go analogue of JT400's per-column
// SQLData converter selection (AS400JDBCPreparedStatement.bindParameter, which
// allocates each parameter's converter directly from the server-declared SQLDA
// rather than retrofitting a Go-type-driven placeholder).
//
// The cache-miss bind paths formerly ran five separate reconciles in sequence
// against the live PMF -- reconcileGraphicBitDataBindShapes,
// reconcileBinaryBindShapes, reconcileDateTimeBindShapes,
// reconcileOutInoutBindShapes, reconcileNullBindShapes -- each keyed on the Go
// value type / driver-chosen SQLType and patched toward the server's declared
// shape. (The cache-HIT path needs none of this: preparedParamsFromCached
// recovers the native shape straight from the *PGM-stored SQLDA, so the PMF is
// already adopted there.) reconcileBindShapesFromPMF collapses those five into
// a single per-slot pass.
//
// STATUS: this dispatcher is the SOLE implementation of cache-miss bind-shape
// reconciliation, live on both cache-miss bind paths -- the EXECUTE/IUD path
// (ExecutePreparedSQL) and the prepared-SELECT predicate path -- and live-
// validated on PUB400 V7R5M0. It replaced the former per-type reconciles, which
// now exist in git history only. Because the downstream encoders
// (EncodeDBExtendedData / ChangeDescriptorRequest / EXECUTE) consume only the
// reconciled paramShapes and paramValues, identical shapes mean identical wire
// bytes. The per-arm field handling is pinned offline by
// TestReconcileBindShapesFromPMFFieldHandling; there is no JT400 wire capture
// for the graphic / date / time / null IN binds (the .trace corpus has no such
// fixture, and the one native-binary capture is unused), so the dispatcher's own
// tests are the offline guard.

// reconcileBindShapesFromPMF adopts the server-declared parameter-marker shape
// for each bind slot in a single pass, choosing the arm from the bind value
// type, the driver's chosen SQLType, and the PMF's declared type/CCSID. It
// returns expectOutput=true when any slot is OUT/INOUT (the OUT/INOUT arm). It
// mutates shapes in place, exactly as the former per-type reconciles did.
//
// The arms are mutually exclusive per slot on the inputs the driver actually
// produces, which is what makes one pass equivalent to the five sequential
// passes:
//
//   - OUT/INOUT slots are gated on the caller's direction byte (0xF1/0xF2); the
//     IN reconciles all skip them.
//   - A []byte or string reaches the graphic arm only for a CCSID-65535 graphic
//     PMF, and a []byte reaches the binary arm only for a CCSID-65535 binary
//     PMF -- isGraphicSQLType and isBinarySQLType are disjoint, so a given slot
//     matches at most one.
//   - The driver binds every time.Time as TIMESTAMP (392/393), so only those
//     slots reach the date/time arm; a []byte/string/nil bind never carries
//     392/393.
//   - The driver binds nil as the INTEGER-NULL marker (497), never 392/393, so
//     a nil value reaches only the NULL arm.
//
// Because of those invariants there is no cross-arm chaining for real inputs
// (e.g. the legacy datetime-then-null double-apply would require a nil value
// carrying a 392/393 shape, which the driver never emits). Each arm reproduces
// its reconcile's exact field handling: graphic/binary copy the PMF
// Precision/Scale, date/time forces Precision/Scale to 0, NULL preserves
// DateFormat, and OUT/INOUT mutates in place (preserving Value/DateFormat).
func reconcileBindShapesFromPMF(shapes []PreparedParam, values []any, pmf []ParameterMarkerField) (expectOutput bool) {
	for i := range shapes {
		// OUT/INOUT direction is independent of PMF length: expectOutput must
		// flip for every OUT/INOUT slot, even one past the end of the PMF whose
		// shape stays the placeholder (matches reconcileOutInoutBindShapes).
		if shapes[i].ParamType == 0xF1 || shapes[i].ParamType == 0xF2 {
			expectOutput = true
			if i < len(pmf) {
				p := pmf[i]
				shapes[i].SQLType = p.SQLType
				shapes[i].FieldLength = p.FieldLength
				shapes[i].Precision = p.Precision
				shapes[i].Scale = p.Scale
				shapes[i].CCSID = p.CCSID
			}
			continue
		}
		if i >= len(pmf) {
			continue
		}
		p := pmf[i]
		switch {
		case isByteOrStringValue(values[i]) && p.CCSID == ccsidBinary && isGraphicSQLType(p.SQLType):
			// graphic bit-data (reconcileGraphicBitDataBindShapes, issue #13).
			shapes[i] = PreparedParam{
				SQLType:     p.SQLType,
				FieldLength: p.FieldLength,
				Precision:   p.Precision,
				Scale:       p.Scale,
				CCSID:       p.CCSID,
				ParamType:   shapes[i].ParamType,
			}
		case isByteSliceValue(values[i]) && p.CCSID == ccsidBinary && isBinarySQLType(p.SQLType):
			// native BINARY/VARBINARY (reconcileBinaryBindShapes, issue #40).
			shapes[i] = PreparedParam{
				SQLType:     p.SQLType,
				FieldLength: p.FieldLength,
				Precision:   p.Precision,
				Scale:       p.Scale,
				CCSID:       p.CCSID,
				ParamType:   shapes[i].ParamType,
			}
		case (shapes[i].SQLType == 392 || shapes[i].SQLType == 393) && isDateTimeSQLType(p.SQLType):
			// native DATE/TIME (reconcileDateTimeBindShapes, issue #40):
			// Precision/Scale forced to 0 to match the cache-hit descriptor.
			shapes[i] = PreparedParam{
				SQLType:     p.SQLType,
				FieldLength: p.FieldLength,
				CCSID:       p.CCSID,
				ParamType:   shapes[i].ParamType,
			}
		case values[i] == nil && !p.IsLOB():
			// typed NULL (reconcileNullBindShapes, issue #11): preserve
			// DateFormat; LOB NULLs are owned by bindLOBParameters.
			shapes[i] = PreparedParam{
				SQLType:     p.SQLType,
				FieldLength: p.FieldLength,
				Precision:   p.Precision,
				Scale:       p.Scale,
				CCSID:       p.CCSID,
				ParamType:   shapes[i].ParamType,
				DateFormat:  shapes[i].DateFormat,
			}
		}
	}
	return expectOutput
}

// isByteSliceValue reports whether v is a []byte (the native-binary bind
// discriminator).
func isByteSliceValue(v any) bool {
	_, ok := v.([]byte)
	return ok
}

// isByteOrStringValue reports whether v is a []byte or string (the graphic
// bit-data bind discriminator).
func isByteOrStringValue(v any) bool {
	switch v.(type) {
	case []byte, string:
		return true
	}
	return false
}
