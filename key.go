package localcache

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func tableHashKey(table string, hk *dynamodb.AttributeValue, idx string) string {
	var key strings.Builder
	key.WriteString(table)
	if hk != nil {
		key.WriteByte('&')
		writeAV(&key, hk)
	}
	if idx != "" {
		key.WriteByte('#')
		key.WriteString(idx)
	}
	return key.String()
}

func itemKey(table string, key map[string]*dynamodb.AttributeValue, schema []*dynamodb.KeySchemaElement) string {
	var str strings.Builder
	writeItemKey(&str, table, key, schema)
	return str.String()
}

func writeItemKey(str *strings.Builder, table string, key map[string]*dynamodb.AttributeValue, schema []*dynamodb.KeySchemaElement) {
	str.WriteString(table)
	str.WriteByte('$')
	str.WriteString(*schema[0].AttributeName)
	str.WriteByte(':')
	writeAV(str, key[*schema[0].AttributeName])
	if len(schema) > 1 {
		str.WriteByte('/')
		str.WriteString(*schema[1].AttributeName)
		str.WriteByte(':')
		writeAV(str, key[*schema[1].AttributeName])
	}
}

func queryKey(input *dynamodb.QueryInput, schema []*dynamodb.KeySchemaElement) string {
	var key strings.Builder
	if input.Select != nil {
		key.WriteString(*input.Select)
	} else {
		key.WriteByte('*')
	}
	if input.ScanIndexForward == nil || (input.ScanIndexForward != nil && *input.ScanIndexForward == true) {
		key.WriteString(".f ")
	} else {
		key.WriteString(".b ")
	}
	if input.IndexName != nil {
		key.WriteString(*input.IndexName)
		key.WriteByte('#')
	}
	// TODO: KeyConditionExpression
	key.WriteString(*schema[0].AttributeName)
	key.WriteByte('`')
	writeCond(&key, input.KeyConditions[*schema[0].AttributeName])
	if len(input.KeyConditions) > 1 {
		key.WriteByte('&')
		key.WriteString(*schema[1].AttributeName)
		key.WriteByte('`')
		writeCond(&key, input.KeyConditions[*schema[1].AttributeName])
	}
	if len(input.ExclusiveStartKey) > 0 {
		key.WriteByte('@')
		writeItemKey(&key, *input.TableName, input.ExclusiveStartKey, schema)
	}
	if input.FilterExpression != nil {
		key.WriteByte('?')
		writeExpr(&key, *input.FilterExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
	}
	if input.Limit != nil {
		key.WriteByte('|')
		key.WriteString(strconv.FormatInt(*input.Limit, 10))
	}
	return key.String()
}

func scanKey(input *dynamodb.ScanInput, schema []*dynamodb.KeySchemaElement) string {
	var key strings.Builder
	if input.Select != nil {
		key.WriteString(*input.Select)
	} else {
		key.WriteByte('*')
	}
	if input.IndexName != nil {
		key.WriteString(*input.IndexName + "#")
	}
	if len(input.ExclusiveStartKey) > 0 {
		key.WriteByte('@')
		writeItemKey(&key, *input.TableName, input.ExclusiveStartKey, schema)
	}
	if input.FilterExpression != nil {
		key.WriteByte('?')
		writeExpr(&key, *input.FilterExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
	}
	if input.Limit != nil {
		key.WriteByte('|')
		key.WriteString(strconv.FormatInt(*input.Limit, 10))
	}
	return key.String()
}

func writeCond(str *strings.Builder, cond *dynamodb.Condition) {
	str.WriteString(*cond.ComparisonOperator)
	str.WriteByte(' ')
	for i, av := range cond.AttributeValueList {
		if i > 0 {
			str.WriteByte('~')
		}
		writeAV(str, av)
	}
}

func av2str(av *dynamodb.AttributeValue) string {
	var str strings.Builder
	writeAV(&str, av)
	return str.String()
}

func writeAV(w *strings.Builder, av *dynamodb.AttributeValue) {
	if av == nil {
		w.WriteString("<nil>")
	}
	switch {
	case av.B != nil:
		w.Write(av.B)
	case av.BS != nil:
		w.WriteString(fmt.Sprint(av.BS))
	case av.BOOL != nil:
		if *av.BOOL {
			w.WriteString("true")
		}
		w.WriteString("false")
	case av.N != nil:
		w.WriteString(*av.N)
	case av.S != nil:
		w.WriteString(*av.S)
	case av.L != nil:
		w.WriteString("L:")
		for _, item := range av.L {
			writeAV(w, item)
			w.WriteByte(',')
		}
	case av.NS != nil:
		w.WriteString("NS:")
		for _, n := range av.NS {
			w.WriteString(*n)
			w.WriteByte(',')
		}
	case av.SS != nil:
		w.WriteString("SS:")
		for _, s := range av.SS {
			w.WriteString(*s)
			w.WriteByte(',')
		}
	case av.M != nil:
		w.WriteString("M:")
		for k, v := range av.M {
			w.WriteString(k)
			w.WriteByte('=')
			writeAV(w, v)
			w.WriteByte(',')
		}
	case av.NULL != nil:
		w.WriteString("NULL")
	default:
		panic("unsupported av")
	}
}

func writeExpr(w *strings.Builder, exp string, names map[string]*string, vals map[string]*dynamodb.AttributeValue) {
	pairs := make([]string, 0, len(names)*2+len(vals)*2)
	for k, v := range names {
		pairs = append(pairs, k, *v)
	}
	for k, v := range vals {
		pairs = append(pairs, k, av2str(v))
	}
	replacer := strings.NewReplacer(pairs...)
	replacer.WriteString(w, exp)
}

func keyEq(a, b map[string]*dynamodb.AttributeValue) bool {
	if len(a) != len(b) {
		return false
	}
	return keyEqLoose(a, b)
}

func keyEqLoose(a, b map[string]*dynamodb.AttributeValue) bool {
	for k, v := range a {
		other, ok := b[k]
		if !ok {
			return false
		}
		switch {
		case v.S != nil:
			if other.S == nil {
				return false
			}
			if *v.S != *other.S {
				return false
			}
		case v.B != nil:
			if other.B == nil {
				return false
			}
			if !bytes.Equal(v.B, other.B) {
				return false
			}
		case v.N != nil:
			if other.N == nil {
				return false
			}
			if *v.N != *other.N {
				return false
			}
		}
	}
	return true
}
