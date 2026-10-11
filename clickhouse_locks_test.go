package clickhouse_locks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlobalClickHouseLockFactory_InitiallyInitialized(t *testing.T) {
	assert.NotNil(t, clickhouseClientBeanFactory)
}
