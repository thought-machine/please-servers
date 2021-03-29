package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseProper(t *testing.T) {
	var a Action
	err := a.UnmarshalFlag("ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381/122")
	assert.NoError(t, err)
	assert.Equal(t, "ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381", a.Hash)
	assert.Equal(t, 122, a.Size)
}

func TestParseTrailingSlash(t *testing.T) {
	var a Action
	err := a.UnmarshalFlag("ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381/122/")
	assert.NoError(t, err)
	assert.Equal(t, "ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381", a.Hash)
	assert.Equal(t, 122, a.Size)
}

func TestParseShort(t *testing.T) {
	var a Action
	err := a.UnmarshalFlag("ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381")
	assert.NoError(t, err)
	assert.Equal(t, "ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381", a.Hash)
	assert.Equal(t, 147, a.Size)
}

func TestParseFail(t *testing.T) {
	var a Action
	err := a.UnmarshalFlag("thirty-five ham and cheese sandwiches")
	assert.Error(t, err)
}

func TestParseWrongLength(t *testing.T) {
	var a Action
	err := a.UnmarshalFlag("ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf138")
	assert.Error(t, err)
}

func TestParseCurrencyGBP(t *testing.T) {
	var c Currency
	err := c.UnmarshalFlag("GBP2.23")
	assert.NoError(t, err)
	assert.Equal(t, "GBP", c.Denomination)
	assert.Equal(t, 2.23, c.Amount)
}

func TestParseCurrencyPound(t *testing.T) {
	var c Currency
	err := c.UnmarshalFlag("£0.32")
	assert.NoError(t, err)
	assert.Equal(t, "GBP", c.Denomination)
	assert.Equal(t, 0.32, c.Amount)
}

func TestParseCurrencyUSD(t *testing.T) {
	var c Currency
	err := c.UnmarshalFlag("USD99.95")
	assert.NoError(t, err)
	assert.Equal(t, "USD", c.Denomination)
	assert.Equal(t, 99.95, c.Amount)
}

func TestParseCurrencyDollar(t *testing.T) {
	var c Currency
	err := c.UnmarshalFlag("$12.23")
	assert.NoError(t, err)
	assert.Equal(t, "USD", c.Denomination)
	assert.Equal(t, 12.23, c.Amount)
}

func TestParseCurrencyInvalid(t *testing.T) {
	var c Currency
	err := c.UnmarshalFlag("12.23")
	assert.Error(t, err)
}
