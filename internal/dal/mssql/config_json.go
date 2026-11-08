package mssql

import (
	"bytes"
	"encoding/json"
	"strings"
)

// MySQL's JSON functions as the config family uses them, written for SQL
// Server (no file of the MySQL DAL holds these: there they are built in).
// Every JSON column is NVARCHAR(MAX) with an ISJSON check, and OPENJSON
// refuses a scalar, so a document is read only once it is known to be an
// object or an array.

// jsonDoc is col when it holds an object or an array, else NULL — what
// OPENJSON may be given.
func jsonDoc(col string) string {
	return "CASE WHEN ISJSON(" + col + ", OBJECT) = 1 OR ISJSON(" + col + ", ARRAY) = 1 THEN " + col + " END"
}

// jsonLength is MySQL's JSON_LENGTH(col): the members of an object or an
// array, 1 for a scalar (null too), NULL for SQL NULL.
func jsonLength(col string) string {
	return "CASE WHEN " + col + " IS NULL THEN NULL WHEN ISJSON(" + col + ", OBJECT) = 1 OR ISJSON(" + col + ", ARRAY) = 1" +
		" THEN (SELECT COUNT(*) FROM OPENJSON(" + col + ")) ELSE 1 END"
}

// jsonTextAt is MySQL's JSON_EXTRACT(col, '$.key') for a key at the top of
// an object: the member's JSON text (a string quoted, an object or an array
// as written), NULL when there is none.
func jsonTextAt(col, key string) string {
	return "(SELECT TOP (1) CASE j.[type] WHEN 0 THEN N'null' WHEN 1 THEN N'\"' + STRING_ESCAPE(j.[value], 'json') + N'\"' ELSE j.[value] END" +
		" FROM OPENJSON(CASE WHEN ISJSON(" + col + ", OBJECT) = 1 THEN " + col + " END) AS j WHERE j.[key] = N'" + key + "')"
}

// jsonArrayHas is MySQL's JSON_CONTAINS(col, candidate) for an array of
// scalars and a scalar candidate given as JSON text (param): some element
// of the same JSON type and value (numbers by value, strings exactly).
func jsonArrayHas(col, param string) string {
	return "EXISTS (SELECT 1 FROM OPENJSON(CASE WHEN ISJSON(" + col + ", ARRAY) = 1 THEN " + col + " END) AS a" +
		" JOIN OPENJSON(N'[' + " + param + " + N']') AS b ON a.[type] = b.[type]" +
		" AND (a.[value] COLLATE Latin1_General_100_BIN2 = b.[value] COLLATE Latin1_General_100_BIN2" +
		" OR (a.[type] = 2 AND TRY_CAST(a.[value] AS FLOAT) = TRY_CAST(b.[value] AS FLOAT))))"
}

// jsonArrayHasString is JSON_CONTAINS(col, JSON_QUOTE(param)): a string
// element equal to param, exactly.
func jsonArrayHasString(col, param string) string {
	return "EXISTS (SELECT 1 FROM OPENJSON(CASE WHEN ISJSON(" + col + ", ARRAY) = 1 THEN " + col + " END) AS a" +
		" WHERE a.[type] = 1 AND a.[value] COLLATE Latin1_General_100_BIN2 = " + param + " COLLATE Latin1_General_100_BIN2)"
}

// planNodes is a recursive walk of every member, at any depth, of the
// send plans of routes (MySQL's '$**' path): one row a member, with the id
// of its route. A statement that uses it begins "WITH " + planNodes + ….
const planNodes = `plan_nodes (rid, [key], [value], [type]) AS (
	SELECT r.id, j.[key], j.[value], j.[type]
	  FROM routes r CROSS APPLY OPENJSON(CASE WHEN ISJSON(r.send_plan, OBJECT) = 1 OR ISJSON(r.send_plan, ARRAY) = 1 THEN r.send_plan END) AS j
	 WHERE r.send_plan IS NOT NULL
	UNION ALL
	SELECT n.rid, j.[key], j.[value], j.[type]
	  FROM plan_nodes n CROSS APPLY OPENJSON(n.[value]) AS j
	 WHERE n.[type] IN (4, 5))`

// planHasNumber is "some member named key, at any depth, is the number
// param" — MySQL's JSON_CONTAINS(JSON_EXTRACT(send_plan,'$**.key'), CAST(? AS
// JSON)) for a number — over plan_nodes.
func planHasNumber(key, param string) string {
	return "SELECT rid FROM plan_nodes WHERE [key] = N'" + key + "' AND [type] = 2 AND TRY_CAST([value] AS FLOAT) = " + param
}

// mergePatch is MySQL's JSON_MERGE_PATCH(target, patch) (RFC 7396): a patch
// that is not an object replaces the target; a member set to null is
// removed; an object member is merged into the target's member.
func mergePatch(target, patch any) any {
	p, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	t, ok := target.(map[string]any)
	if !ok {
		t = map[string]any{}
	}
	for k, v := range p {
		if v == nil {
			delete(t, k)
			continue
		}
		t[k] = mergePatch(t[k], v)
	}
	return t
}

// decodeJSON reads JSON text keeping numbers as written.
func decodeJSON(s string) (any, error) {
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// encodeJSON writes v as compact JSON, with no HTML escaping.
func encodeJSON(v any) (string, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return "", err
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}
