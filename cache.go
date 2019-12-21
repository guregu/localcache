package localcache

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/karlseguin/ccache"
	// "github.com/davecgh/go-spew/spew"
)

func New(p client.ConfigProvider, cfgs ...*aws.Config) dynamodbiface.DynamoDBAPI {
	db := dynamodb.New(p, cfgs...)
	return NewWithDB(db)
}

func NewWithDB(client *dynamodb.DynamoDB) dynamodbiface.DynamoDBAPI {
	return &Cache{
		DynamoDB: client,

		items:     ccache.New(ccache.Configure()),
		tableDesc: ccache.New(ccache.Configure()),
		queries:   ccache.Layered(ccache.Configure()),
		scans:     ccache.Layered(ccache.Configure()),
	}
}

type Cache struct {
	*dynamodb.DynamoDB

	items     *ccache.Cache
	tableDesc *ccache.Cache
	queries   *ccache.LayeredCache
	scans     *ccache.LayeredCache

	Debug bool
}

func (c *Cache) log(v ...interface{}) {
	if c.Debug {
		log.Println(v...)
	}
}

func (c *Cache) getItem(key string) (interface{}, bool) {
	item := c.items.Get(key)
	if item == nil {
		return nil, false
	}
	return item.Value(), true
}

func (c *Cache) setItem(key string, v interface{}) {
	c.items.Set(key, v, 5*time.Minute)
}

func (c *Cache) deleteItem(key string) {
	c.items.Delete(key)
}

func (c *Cache) getQuery(table, key string) (interface{}, bool) {
	item := c.queries.Get(table, key)
	if item == nil {
		return nil, false
	}
	return item.Value(), true
}

func (c *Cache) setQuery(table, key string, v interface{}) {
	c.queries.Set(table, key, v, 5*time.Minute)
}

func (c *Cache) getScan(table, key string) (interface{}, bool) {
	item := c.scans.Get(table, key)
	if item == nil {
		return nil, false
	}
	return item.Value(), true
}

func (c *Cache) setScan(table, key string, v interface{}) {
	c.scans.Set(table, key, v, 5*time.Minute)
}

func (c *Cache) invalidate(table string, item map[string]*dynamodb.AttributeValue) {
	desc, err := c.desc(table)
	if err != nil {
		panic(err)
	}
	c.scans.DeleteAll(table)
	if len(desc.Table.KeySchema) == 1 {
		c.queries.DeleteAll(table)
	} else {
		key := tableHashKey(table, av2str(item[*desc.Table.KeySchema[0].AttributeName]), "")
		c.log("invalidate", key)
		c.queries.DeleteAll(key)
	}
	for _, gsi := range desc.Table.GlobalSecondaryIndexes {
		if len(gsi.KeySchema) == 1 {
			key := tableHashKey(table, "", *gsi.IndexName)
			c.log("invalidate", key)
			c.queries.DeleteAll(key)
		} else if hk, ok := item[*gsi.KeySchema[0].AttributeName]; ok {
			key := tableHashKey(table, av2str(hk), *gsi.IndexName)
			c.log("invalidate", key)
			c.queries.DeleteAll(key)
		}
	}
	for _, lsi := range desc.Table.LocalSecondaryIndexes {
		if hk, ok := item[*lsi.KeySchema[0].AttributeName]; ok {
			key := tableHashKey(table, av2str(hk), *lsi.IndexName)
			c.log("invalidate", key)
			c.queries.DeleteAll(key)
		}
	}
}

func tableHashKey(table, hk, idx string) string {
	key := table
	if hk != "" {
		key += "&" + hk
	}
	if idx != "" {
		key += "#" + idx
	}
	return key
}

func (c *Cache) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	// spew.Dump(input)
	schema, err := c.schemaOf(*input.TableName)
	if err != nil {
		return nil, err
	}
	key := itemKey(*input.TableName, input.Key, schema)
	if item, ok := c.getItem(key); ok {
		c.log("returning cached", key)
		return &dynamodb.GetItemOutput{
			Item: item.(map[string]*dynamodb.AttributeValue),
		}, nil
	}
	out, err := c.DynamoDB.GetItemWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}
	c.log("caching", key)
	c.setItem(key, out.Item)
	return out, err
}

func (c *Cache) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	schema, err := c.schemaOf(*input.TableName)
	if err != nil {
		return nil, err
	}
	key := itemKey(*input.TableName, input.Item, schema)

	out, err := c.DynamoDB.PutItemWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}
	c.log("caching put", key)
	c.setItem(key, input.Item)
	c.invalidate(*input.TableName, input.Item)
	return out, err
}

func (c *Cache) DeleteItemWithContext(ctx aws.Context, input *dynamodb.DeleteItemInput, opts ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	schema, err := c.schemaOf(*input.TableName)
	if err != nil {
		return nil, err
	}

	out, err := c.DynamoDB.DeleteItemWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}

	key := itemKey(*input.TableName, input.Key, schema)
	c.deleteItem(key)
	c.invalidate(*input.TableName, input.Key)
	c.log("deleting cached", key)

	return out, err
}

func (c *Cache) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	schema, err := c.schemaOf(*input.TableName)
	if err != nil {
		return nil, err
	}

	// TODO: undo this later maybe
	if input.ReturnValues == nil || *input.ReturnValues == dynamodb.ReturnValueNone {
		input.ReturnValues = aws.String(dynamodb.ReturnValueAllNew)
	}

	out, err := c.DynamoDB.UpdateItemWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}

	key := itemKey(*input.TableName, input.Key, schema)
	if input.ReturnValues != nil && *input.ReturnValues == dynamodb.ReturnValueAllNew {
		c.log("cache updated", key)
		c.setItem(key, out.Attributes)
		c.invalidate(*input.TableName, out.Attributes)
	} else {
		c.log("delete updated", key)
		c.deleteItem(key)
		// TODO: this could miss invalidating an index...
		c.invalidate(*input.TableName, input.Key)
	}
	return out, err
}

func (c *Cache) BatchGetItemWithContext(ctx aws.Context, input *dynamodb.BatchGetItemInput, opts ...request.Option) (*dynamodb.BatchGetItemOutput, error) {
	schemas := make(map[string][]*dynamodb.KeySchemaElement)
	fake := &dynamodb.BatchGetItemOutput{
		Responses:       make(map[string][]map[string]*dynamodb.AttributeValue),
		UnprocessedKeys: make(map[string]*dynamodb.KeysAndAttributes),
	}
	var newReq map[string]*dynamodb.KeysAndAttributes
	for table, req := range input.RequestItems {
		schema, ok := schemas[table]
		if !ok {
			var err error
			schema, err = c.schemaOf(table)
			if err != nil {
				return nil, err
			}
			schemas[table] = schema
		}

		var newKeys []map[string]*dynamodb.AttributeValue

		for _, k := range req.Keys {
			key := itemKey(table, k, schema)
			if item, ok := c.getItem(key); ok {
				c.log("batch get cached", key)
				fake.Responses[table] = append(fake.Responses[table], item.(map[string]*dynamodb.AttributeValue))
			} else {
				newKeys = append(newKeys, k)
			}
		}

		if len(newKeys) > 0 {
			if newReq == nil {
				newReq = make(map[string]*dynamodb.KeysAndAttributes)
			}
			req.Keys = newKeys
			newReq[table] = req
		}
	}

	if len(newReq) == 0 {
		return fake, nil
	}

	input.RequestItems = newReq
	out, err := c.DynamoDB.BatchGetItemWithContext(ctx, input, opts...)
	if err != nil {
		return nil, err
	}

	for table, resp := range out.Responses {
		for _, item := range resp {
			key := itemKey(table, item, schemas[table])
			c.log("batch get caching", key)
			c.setItem(key, item)
		}
	}

	if len(fake.Responses) == 0 {
		return out, err
	}

	for table, resp := range fake.Responses {
		out.Responses[table] = append(out.Responses[table], resp...)
	}

	return out, nil
}

func (c *Cache) BatchWriteItemWithContext(ctx aws.Context, input *dynamodb.BatchWriteItemInput, opts ...request.Option) (*dynamodb.BatchWriteItemOutput, error) {
	out, err := c.DynamoDB.BatchWriteItemWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}
	for table, reqs := range input.RequestItems {
		schema, err := c.schemaOf(table)
		if err != nil {
			// TODO: probably bad to error out here
			return out, err
		}
	next:
		for _, req := range reqs {
			if req.DeleteRequest != nil {
				for _, unprocessed := range out.UnprocessedItems[table] {
					if unprocessed.DeleteRequest == nil {
						continue
					}
					if keyEq(unprocessed.DeleteRequest.Key, req.DeleteRequest.Key) {
						continue next
					}
				}
				key := itemKey(table, req.DeleteRequest.Key, schema)
				c.log("batch delete", key)
				c.deleteItem(key)
				c.invalidate(table, req.DeleteRequest.Key)
			} else if req.PutRequest != nil {
				for _, unprocessed := range out.UnprocessedItems[table] {
					if unprocessed.PutRequest == nil {
						continue
					}
					if keyEq(unprocessed.PutRequest.Item, req.PutRequest.Item) {
						continue next
					}
				}
				key := itemKey(table, req.PutRequest.Item, schema)
				c.log("batch put", key)
				c.setItem(key, req.PutRequest.Item)
				c.invalidate(table, req.PutRequest.Item)
			}
		}
	}
	return out, err
}

func (c *Cache) TransactWriteItemsWithContext(ctx aws.Context, input *dynamodb.TransactWriteItemsInput, opts ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
	out, err := c.DynamoDB.TransactWriteItemsWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}
	for _, req := range input.TransactItems {
		switch {
		case req.Put != nil:
			schema, err := c.schemaOf(*req.Put.TableName)
			if err != nil {
				return out, err
			}
			key := itemKey(*req.Put.TableName, req.Put.Item, schema)
			c.log("transact put", key)
			c.setItem(key, req.Put.Item)
			c.invalidate(*req.Put.TableName, req.Put.Item)
		case req.Delete != nil:
			schema, err := c.schemaOf(*req.Delete.TableName)
			if err != nil {
				return out, err
			}
			key := itemKey(*req.Delete.TableName, req.Delete.Key, schema)
			c.log("transact delete", key)
			c.deleteItem(key)
			c.invalidate(*req.Delete.TableName, req.Delete.Key)
		case req.Update != nil:
			schema, err := c.schemaOf(*req.Update.TableName)
			if err != nil {
				return out, err
			}
			key := itemKey(*req.Update.TableName, req.Update.Key, schema)
			c.log("transact update", key)
			c.deleteItem(key)
			c.invalidate(*req.Update.TableName, req.Update.Key)
		}
	}
	return out, err
}

func (c *Cache) QueryWithContext(ctx aws.Context, input *dynamodb.QueryInput, opts ...request.Option) (*dynamodb.QueryOutput, error) {
	// spew.Dump(input)
	var idx string
	var schema []*dynamodb.KeySchemaElement
	var err error
	if input.IndexName == nil {
		schema, err = c.schemaOf(*input.TableName)
	} else {
		schema, err = c.schemaOfIndex(*input.TableName, *input.IndexName)
		idx = *input.IndexName
	}
	if err != nil {
		return nil, err
	}
	var tkey string
	if len(schema) == 1 {
		tkey = tableHashKey(*input.TableName, "", idx)
	} else {
		tkey = tableHashKey(*input.TableName, av2str(input.KeyConditions[*schema[0].AttributeName].AttributeValueList[0]), idx)
	}
	key := queryKey(input, schema)
	if out, ok := c.getQuery(tkey, key); ok {
		c.log("cached query:", tkey, key)
		return out.(*dynamodb.QueryOutput), nil
	}

	out, err := c.DynamoDB.QueryWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}
	c.log("saving query:", tkey, key)
	c.setQuery(tkey, key, out)
	return out, err
}

func queryKey(input *dynamodb.QueryInput, schema []*dynamodb.KeySchemaElement) string {
	var key string
	if input.Select != nil {
		key = *input.Select
	} else {
		key = "*"
	}
	if input.ScanIndexForward == nil || (input.ScanIndexForward != nil && *input.ScanIndexForward == true) {
		key += ".f "
	} else {
		key += ".b "
	}
	if input.IndexName != nil {
		key += *input.IndexName + "#"
	}
	// TODO: KeyConditionExpression
	key += *schema[0].AttributeName + cond2str(input.KeyConditions[*schema[0].AttributeName])
	if len(input.KeyConditions) > 1 {
		key += "&" + *schema[1].AttributeName + cond2str(input.KeyConditions[*schema[1].AttributeName])
	}
	if len(input.ExclusiveStartKey) > 0 {
		key += "@" + itemKey(*input.TableName, input.ExclusiveStartKey, schema)
	}
	if input.FilterExpression != nil {
		key += "?" + exp2str(*input.FilterExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
	}
	if input.Limit != nil {
		key += "|" + strconv.FormatInt(*input.Limit, 10)
	}
	return key
}

func (c *Cache) ScanWithContext(ctx aws.Context, input *dynamodb.ScanInput, opts ...request.Option) (*dynamodb.ScanOutput, error) {
	schema, err := c.schemaOf(*input.TableName)
	if err != nil {
		return nil, err
	}

	key := scanKey(input, schema)
	if out, ok := c.getScan(*input.TableName, key); ok {
		c.log("returning cached scan", key)
		return out.(*dynamodb.ScanOutput), nil
	}

	out, err := c.DynamoDB.ScanWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}
	c.log("caching scan", key)
	c.setScan(*input.TableName, key, out)
	return out, err
}

func scanKey(input *dynamodb.ScanInput, schema []*dynamodb.KeySchemaElement) string {
	var key string
	if input.Select != nil {
		key += *input.Select
	} else {
		key += "*"
	}
	if input.IndexName != nil {
		key += *input.IndexName + "#"
	}
	if len(input.ExclusiveStartKey) > 0 {
		key += "@" + itemKey(*input.TableName, input.ExclusiveStartKey, schema)
	}
	if input.FilterExpression != nil {
		key += "?" + exp2str(*input.FilterExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
	}
	if input.Limit != nil {
		key += "|" + strconv.FormatInt(*input.Limit, 10)
	}
	return key
}

func exp2str(exp string, names map[string]*string, vals map[string]*dynamodb.AttributeValue) string {
	for k, v := range names {
		exp = strings.Replace(exp, k, *v, -1)
	}
	for k, v := range vals {
		exp = strings.Replace(exp, k, av2str(v), -1)
	}
	return exp
}

func cond2str(cond *dynamodb.Condition) string {
	switch *cond.ComparisonOperator {
	case dynamodb.ComparisonOperatorEq:
		return "=" + av2str(cond.AttributeValueList[0])
	case dynamodb.ComparisonOperatorNe:
		return "!=" + av2str(cond.AttributeValueList[0])
	case dynamodb.ComparisonOperatorLt:
		return "<" + av2str(cond.AttributeValueList[0])
	case dynamodb.ComparisonOperatorLe:
		return "<=" + av2str(cond.AttributeValueList[0])
	case dynamodb.ComparisonOperatorGt:
		return ">" + av2str(cond.AttributeValueList[0])
	case dynamodb.ComparisonOperatorGe:
		return ">=" + av2str(cond.AttributeValueList[0])
	case dynamodb.ComparisonOperatorBeginsWith:
		return "bw(" + av2str(cond.AttributeValueList[0]) + ")"
	case dynamodb.ComparisonOperatorBetween:
		return av2str(cond.AttributeValueList[0]) + "~" + av2str(cond.AttributeValueList[1])
	}
	panic("unknown cond " + *cond.ComparisonOperator)
}

func (c *Cache) schemaOf(table string) ([]*dynamodb.KeySchemaElement, error) {
	desc, err := c.desc(table)
	if err != nil {
		return nil, err
	}
	return desc.Table.KeySchema, nil
}

func (c *Cache) schemaOfIndex(table, index string) ([]*dynamodb.KeySchemaElement, error) {
	desc, err := c.desc(table)
	if err != nil {
		return nil, err
	}

	for _, gsi := range desc.Table.GlobalSecondaryIndexes {
		if *gsi.IndexName == index {
			return gsi.KeySchema, nil
		}
	}
	for _, lsi := range desc.Table.LocalSecondaryIndexes {
		if *lsi.IndexName == index {
			return lsi.KeySchema, nil
		}
	}

	panic("index not found: " + table + " " + index)
}

func (c *Cache) desc(table string) (*dynamodb.DescribeTableOutput, error) {
	item := c.tableDesc.Get(table)
	if item == nil {
		out, err := c.DynamoDB.DescribeTable(&dynamodb.DescribeTableInput{TableName: &table})
		if err != nil {
			return nil, err
		}
		c.tableDesc.Set(table, out, 24*time.Hour)
		c.log("caching desc", out)
		return out, nil
	}
	return item.Value().(*dynamodb.DescribeTableOutput), nil
}

func itemKey(table string, key map[string]*dynamodb.AttributeValue, schema []*dynamodb.KeySchemaElement) string {
	if len(schema) == 1 {
		return table + "$" + *schema[0].AttributeName + ":" + av2str(key[*schema[0].AttributeName])
	}
	return table + "$" + *schema[0].AttributeName + ":" + av2str(key[*schema[0].AttributeName]) + "/" +
		*schema[1].AttributeName + ":" + av2str(key[*schema[1].AttributeName])
}

func keyEq(a, b map[string]*dynamodb.AttributeValue) bool {
	if len(a) != len(b) {
		return false
	}
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

func av2str(av *dynamodb.AttributeValue) string {
	switch {
	case av.B != nil:
		return string(av.B)
	case av.BS != nil:
		return fmt.Sprint(av.BS)
	case av.BOOL != nil:
		if *av.BOOL {
			return "true"
		}
		return "false"
	case av.N != nil:
		return *av.N
	case av.S != nil:
		return *av.S
	case av.L != nil:
		ret := "L:"
		for _, item := range av.L {
			ret += av2str(item) + ","
		}
		return ret
	case av.NS != nil:
		ret := "NS:"
		for _, n := range av.NS {
			ret += *n + ","
		}
		return ret
	case av.SS != nil:
		ret := "SS:"
		for _, s := range av.SS {
			ret += *s + ","
		}
		return ret
	case av.M != nil:
		ret := "M:"
		for k, v := range av.M {
			ret += k + "=" + av2str(v) + ","
		}
		return ret
	case av.NULL != nil:
		return "NULL"
	}
	panic("unsupported av")
}
