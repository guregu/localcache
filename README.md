## localcache [![GoDoc](https://godoc.org/github.com/guregu/localcache?status.svg)](https://godoc.org/github.com/guregu/localcache)
`import "github.com/guregu/localcache"`

This is a DynamoDB wrapper that caches values in memory. Very experimental, don't use it.

### Problems
* This library has only been tested with `guregu/dynamo`, so it doesn't support things like `KeyConditionExpression`.
* Projections don't work and will probably break lots of stuff.
* Cache isn't configurable and ~~doesn't expire properly~~.
* Query cache for certain kinds of indexes won't be invalidated properly through certain operations

Perhaps one day I'll fix these!
