package localcache

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/patrickmn/go-cache"

	_ "github.com/davecgh/go-spew/spew"
)

func New(p client.ConfigProvider, cfgs ...*aws.Config) dynamodbiface.DynamoDBAPI {
	db := dynamodb.New(p, cfgs...)
	return NewWithDB(db)
}

func NewWithDB(client *dynamodb.DynamoDB) dynamodbiface.DynamoDBAPI {
	return &Cache{
		DynamoDB: client,

		items:     cache.New(5*time.Minute, 10*time.Minute),
		tableDesc: cache.New(cache.NoExpiration, cache.NoExpiration),
	}
}

type Cache struct {
	*dynamodb.DynamoDB
	items     *cache.Cache
	tableDesc *cache.Cache
}

func (c *Cache) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	// spew.Dump(input)
	schema, err := c.schemaOf(*input.TableName)
	if err != nil {
		return nil, err
	}
	key := itemKey(*input.TableName, input.Key, schema)
	if item, ok := c.items.Get(key); ok {
		log.Print("returning cached", key)
		return &dynamodb.GetItemOutput{
			Item: item.(map[string]*dynamodb.AttributeValue),
		}, nil
	}
	out, err := c.DynamoDB.GetItemWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}
	log.Print("caching", key)
	c.items.Set(key, out.Item, cache.DefaultExpiration)
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
	log.Println("caching put", key)
	c.items.Set(key, input.Item, cache.DefaultExpiration)
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
	c.items.Delete(key)
	log.Println("deleting cached", key)

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
		log.Println("cache updated", key)
		c.items.Set(key, out.Attributes, cache.DefaultExpiration)
	} else {
		log.Println("delete updated", key)
		c.items.Delete(key)
	}
	return out, err
}

func (c *Cache) schemaOf(table string) ([]*dynamodb.KeySchemaElement, error) {
	desc, ok := c.tableDesc.Get(table)
	if !ok {
		out, err := c.DynamoDB.DescribeTable(&dynamodb.DescribeTableInput{TableName: &table})
		if err != nil {
			return nil, err
		}
		c.tableDesc.Set(table, out, cache.DefaultExpiration)
		log.Println("caching desc", out)
		desc = out
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
