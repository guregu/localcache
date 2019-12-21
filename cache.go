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

		getItem: cache.New(5*time.Minute, 10*time.Minute),
	}
}

type Cache struct {
	*dynamodb.DynamoDB
	getItem *cache.Cache
}

func (c *Cache) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	// spew.Dump(input)
	key := *input.TableName + "$" + key2str(input.Key)
	// key := fmt.Sprintf("%s %v"), *input.TableName, *input.)
	if out, ok := c.getItem.Get(key); ok {
		log.Print("returning cached", key)
		return out.(*dynamodb.GetItemOutput), nil
	}
	out, err := c.DynamoDB.GetItemWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}
	log.Print("caching", key)
	c.getItem.Set(key, out, cache.DefaultExpiration)
	return out, err
}

func key2str(key map[string]*dynamodb.AttributeValue) string {
	if len(key) == 1 {
		for k, v := range key {
			return k + ":" + av2str(v)
		}
	}
	var a, b string
	for k, v := range key {
		if a == "" {
			a = k + ":" + av2str(v)
		}
		b = k + ":" + av2str(v)
	}
	if a[0] < b[0] {
		return a + "/" + b
	}
	return b + "/" + a
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
