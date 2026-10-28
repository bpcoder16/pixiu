package moneyx_test

import (
	"errors"
	"fmt"

	"github.com/bpcoder16/pixiu/moneyx"
)

func ExampleYuanToCent() {
	cent, err := moneyx.YuanToCent("-12.30")
	fmt.Println(cent, err)

	_, err = moneyx.YuanToCent("92233720368547758.08")
	fmt.Println(errors.Is(err, moneyx.ErrOverflow))
	// Output:
	// -1230 <nil>
	// true
}

func ExampleCentToYuan() {
	fmt.Println(moneyx.CentToYuan(1230))
	fmt.Println(moneyx.CentToYuan(1))
	fmt.Println(moneyx.CentToYuan(-1))
	// Output:
	// 12.30
	// 0.01
	// -0.01
}
