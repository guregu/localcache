package localcache

import (
	"bytes"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	// "github.com/patrickmn/go-cache"
	"github.com/karlseguin/ccache"
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
	}
}

type Cache struct {
	*dynamodb.DynamoDB
	items     *ccache.Cache
	tableDesc *ccache.Cache

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
	} else {
		c.log("delete updated", key)
		c.deleteItem(key)
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
		case req.Delete != nil:
			schema, err := c.schemaOf(*req.Delete.TableName)
			if err != nil {
				return out, err
			}
			key := itemKey(*req.Delete.TableName, req.Delete.Key, schema)
			c.log("transact delete", key)
			c.deleteItem(key)
		case req.Update != nil:
			schema, err := c.schemaOf(*req.Update.TableName)
			if err != nil {
				return out, err
			}
			key := itemKey(*req.Update.TableName, req.Update.Key, schema)
			c.log("transact update", key)
			c.deleteItem(key)
		}
	}
	return out, err
}

func (c *Cache) schemaOf(table string) ([]*dynamodb.KeySchemaElement, error) {
	item := c.tableDesc.Get(table)
	var desc interface{}
	if item == nil {
		out, err := c.DynamoDB.DescribeTable(&dynamodb.DescribeTableInput{TableName: &table})
		if err != nil {
			return nil, err
		}
		c.tableDesc.Set(table, out, time.Minute*5)
		c.log("caching desc", out)
		desc = out
	} else {
		desc = item.Value()
	}
	return desc.(*dynamodb.DescribeTableOutput).Table.KeySchema, nil
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
	case av.S != nil:
		return *av.S
	case av.B != nil:
		return string(av.B)
	case av.N != nil:
		return *av.N
	}
	panic("invalid key av")
}
