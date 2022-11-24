package ml

import (
	"fmt"
)

func Predict_fn(data []float64) int {
	o, err := NewAutogluon("ml", "predict_autogluon") // ml : name of the file (important)

	if err != nil {
		fmt.Println("err1")
		return -1
	}
	defer o.Close()
	ans, err := o.Predict(data)
	if err != nil {
		return -1
	}
	return (int(ans[0]))
}

func Learn_fn() {

	o, err := NewAutogluon("ml", "learn_data") // ml : name of the file (important)
	fmt.Println("Hello")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer o.Close()
	o.Learn()
}
