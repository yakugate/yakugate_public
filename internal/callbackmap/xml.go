package callbackmap

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"strings"
)

// decodeXML turns an XML body into the same tree a JSON body makes, so one
// path syntax reads both:
//
//	<DeliveryReport id="7"><MessageId>abc</MessageId><Status>1</Status></DeliveryReport>
//
// becomes {"DeliveryReport": {"@id": "7", "MessageId": "abc", "Status": "1"}}.
// Names are LOCAL names — a SOAP body's namespace prefixes are dropped, so
// "Envelope.Body.Report.Id" reads a SOAP callback. An element that repeats
// becomes a list; an element with attributes keeps its text under "#text".
func decodeXML(body []byte) (any, error) {
	dec := xml.NewDecoder(bytes.NewReader(body))
	dec.Strict = false
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		if se, ok := tok.(xml.StartElement); ok {
			v, err := element(dec, se, 0)
			if err != nil {
				return nil, err
			}
			return map[string]any{se.Name.Local: v}, nil
		}
	}
}

// maxDepth bounds a hostile body; a real callback is a few levels deep.
const maxDepth = 32

func element(dec *xml.Decoder, se xml.StartElement, depth int) (any, error) {
	if depth > maxDepth {
		return nil, errors.New("xml nested too deeply")
	}
	node := map[string]any{}
	for _, a := range se.Attr {
		node["@"+a.Name.Local] = a.Value
	}
	var text strings.Builder
	children := false
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil, errors.New("xml ended early")
		}
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			children = true
			v, err := element(dec, t, depth+1)
			if err != nil {
				return nil, err
			}
			name := t.Name.Local
			switch prev := node[name].(type) {
			case nil:
				node[name] = v
			case []any:
				node[name] = append(prev, v)
			default:
				node[name] = []any{prev, v}
			}
		case xml.CharData:
			text.Write(t)
		case xml.EndElement:
			s := strings.TrimSpace(text.String())
			if !children && len(se.Attr) == 0 {
				return s, nil
			}
			if s != "" {
				node["#text"] = s
			}
			return node, nil
		}
	}
}
