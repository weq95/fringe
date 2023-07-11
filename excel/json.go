package excel

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"fringe/cfg"
	"os"
	"reflect"
	"strconv"
	"sync"
)

type JsonToStruct struct {
	lock *sync.RWMutex
	data *JsonData
}

type JsonData struct {
	Test []Test
}

var result = &JsonToStruct{
	lock: new(sync.RWMutex),
	data: new(JsonData),
}

// 后期可能改为反射,进行数据解析
func loadJsonToExcel() error {
	var value = new(JsonData)
	if err := ReadJsonFileContent(value); err != nil {
		return err
	}

	result.lock.Lock()
	result.data = value
	result.lock.Unlock()
	return nil
}

func ReadJsonFileContent(v interface{}) (err error) {
	defer func() {
		if err01 := recover(); err01 != nil {
			err = fmt.Errorf("reflect panic: %v", err01)
		}
	}()

	var typeVal = reflect.TypeOf(v)
	if typeVal.Kind() != reflect.Ptr {
		return errors.New("`v` variable must be of pointer type")
	}

	var valueType = reflect.ValueOf(v).Elem()
	var typeElem = typeVal.Elem()
	for i := 0; i < typeElem.NumField(); i++ {
		var fieldName = typeElem.Field(i).Name
		var content []byte
		content, err = GetFileContent(cfg.JsonFilePath + fieldName + ".json")
		if err != nil {
			return err
		}

		var fieldType = valueType.Field(i)
		var fieldValAddr = valueType.Field(i).Addr().Interface()
		switch fieldType.Type().Kind() {
		case reflect.Struct:
			var data map[string]interface{}
			if err = json.Unmarshal(content, &data); err != nil {
				return err
			}
			if err = fillStruct(fieldValAddr, data); err != nil {
				return err
			}
		case reflect.Slice:
			var data []interface{}
			if err = json.Unmarshal(content, &data); err != nil {
				return err
			}
			if err = fillSlice(fieldValAddr, data); err != nil {
				return err
			}
		case reflect.Map:
			var data map[string]interface{}
			if err = json.Unmarshal(content, &data); err != nil {
				return err
			}
		case reflect.Array:
			panic("--------- 等待处理 ---------")
		default:
			if err = fillPrimitive(fieldValAddr, nil); err != nil {
				return err
			}
		}
	}

	return err
}

func GetFileContent(filename string) ([]byte, error) {
	var osFile, err = os.Open(filename)
	if err != nil {
		return nil, err
	}
	var buffer = new(bytes.Buffer)
	_, err = buffer.ReadFrom(osFile)
	_ = osFile.Close()
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), err
}

func fillStruct(data interface{}, result map[string]interface{}) error {
	var dataType = reflect.TypeOf(data).Elem()
	var dataValue = reflect.ValueOf(data).Elem()

	var err error
	for i := 0; i < dataType.NumField(); i++ {
		var fieldType = dataType.Field(i)
		var fieldValue = dataValue.Field(i).Addr().Interface()

		var fieldName = fieldType.Tag.Get("json")
		if fieldName == "" {
			fieldName = fieldType.Name
		}

		switch fieldType.Type.Kind() {
		case reflect.Struct:
			var newStruct, ok = result[fieldName].(map[string]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式错误：%s.(map[string]any)", fieldName))
			}
			if err = fillStruct(fieldValue, newStruct); err != nil {
				return err
			}
		case reflect.Map:
			var newMap, ok = result[fieldName]
			if false == ok {
				return errors.New(fmt.Sprintf("%s field not found", fieldName))
			}
			if err = fillMap(fieldValue, newMap.(map[string]any)); err != nil {
				return err
			}
		case reflect.Slice:
			var newSlice, ok = result[fieldName].([]interface{})
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式错误：%s.([]any)", fieldName))
			}

			if err = fillSlice(fieldValue, newSlice); err != nil {
				return err
			}
		case reflect.Array:
			var newArray, ok = result[fieldName].([]interface{})
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式错误：%s.([]any)", fieldName))
			}

			if err = fillArray(fieldValue, newArray); err != nil {
				return err
			}
		default:
			var value, ok = result[fieldName]
			if false == ok {
				return errors.New(fmt.Sprintf("%s field not found", fieldName))
			}
			if reflect.ValueOf(value).Kind() == reflect.Interface {
				dataValue.Set(reflect.ValueOf(value))
			} else {
				if err = fillPrimitive(fieldValue, value); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func fillSlice(data interface{}, value []interface{}) error {
	var dataValue = reflect.ValueOf(data).Elem()
	var err error
	if dataValue.Len() == 0 {
		dataValue.Set(reflect.MakeSlice(dataValue.Type(), len(value), len(value)))
	}
	if len(value) > dataValue.Len() {
		return errors.New("fillSlice value.Len > data.Len 数组越界")
	}

	for i := 0; i < dataValue.Len(); i++ {
		var itemValue = dataValue.Index(i)
		var itemValAdd = itemValue.Addr().Interface()

		switch itemValue.Kind() {
		case reflect.Struct:
			var newStruct, ok = value[i].(map[string]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式不正确：%s.(map[string]any)", value[i]))
			}
			if err = fillStruct(itemValAdd, newStruct); err != nil {
				return err
			}
		case reflect.Slice:
			var newSlice, ok = value[i].([]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式不正确：%s.([]any)", value[i]))
			}
			if err = fillSlice(itemValAdd, newSlice); err != nil {
				return err
			}
		case reflect.Array:
			var newArray, ok = value[i].([]interface{})
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式错误：%s.([]any)", i))
			}
			if err = fillArray(itemValAdd, newArray); err != nil {
				return err
			}
		case reflect.Map:
			var newMap, ok = value[i].(map[string]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式不正确：%s.(map[string]any)", value[i]))
			}
			if err = fillMap(itemValAdd, newMap); err != nil {
				return err
			}
		default:
			if itemValue.Kind() == reflect.Interface {
				var arrayVal = reflect.ValueOf(value[i])
				if arrayVal.Kind() == reflect.Ptr {
					arrayVal = arrayVal.Elem()
				}
				itemValue.Set(arrayVal)
			} else {
				if err = fillPrimitive(itemValAdd, value[i]); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func fillMap(data interface{}, mapVal map[string]interface{}) error {
	var dataValue = reflect.ValueOf(data).Elem()
	var keys = dataValue.MapKeys()
	if len(keys) == 0 {
		var newMap = reflect.MakeMapWithSize(dataValue.Type(), len(mapVal))
		dataValue.Set(newMap)

		for key, val := range mapVal {
			var value = reflect.ValueOf(val)
			var mapKey reflect.Value

			if !value.IsValid() || value.Kind() == reflect.Ptr && value.IsNil() {
				return errors.New(fmt.Sprintf("%s: %+v 的值不允许设置为nil", key, val))
			}

			if value.Kind() == reflect.Ptr {
				value = value.Elem()
			}

			switch dataValue.Type().Key().Kind() {
			case reflect.Int:
				var keyInt, _ = strconv.Atoi(key)
				mapKey = reflect.ValueOf(keyInt)
			case reflect.Int8:
				var keyInt, _ = strconv.Atoi(key)
				mapKey = reflect.ValueOf(int8(keyInt))
			case reflect.Int16:
				var keyInt, _ = strconv.Atoi(key)
				mapKey = reflect.ValueOf(int16(keyInt))
			case reflect.Int32:
				var keyInt, _ = strconv.Atoi(key)
				mapKey = reflect.ValueOf(int32(keyInt))
			case reflect.Int64:
				var keyInt, _ = strconv.Atoi(key)
				mapKey = reflect.ValueOf(int64(keyInt))
			case reflect.Uint:
				var keyInt, _ = strconv.Atoi(key)
				mapKey = reflect.ValueOf(uint(keyInt))
			case reflect.Uint8:
				var keyInt, _ = strconv.Atoi(key)
				mapKey = reflect.ValueOf(uint8(keyInt))
			case reflect.Uint16:
				var keyInt, _ = strconv.Atoi(key)
				mapKey = reflect.ValueOf(uint16(keyInt))
			case reflect.Uint32:
				var keyInt, _ = strconv.Atoi(key)
				mapKey = reflect.ValueOf(uint32(keyInt))
			case reflect.Uint64:
				var keyInt, _ = strconv.Atoi(key)
				mapKey = reflect.ValueOf(uint64(keyInt))
			case reflect.Bool:
				var keyBool, _ = strconv.ParseBool(key)
				mapKey = reflect.ValueOf(keyBool)
			case reflect.String:
				mapKey = reflect.ValueOf(key)
			default:
				return errors.New("不受支持的 map[key] 类型")
			}

			var valueInterface = func() (error, reflect.Value) {
				var structType = dataValue.Type().Elem()
				var newValueRes = reflect.New(structType).Elem()
				switch newValueRes.Kind() {
				case reflect.Slice:
					var newSliceVal, ok = mapVal[key].([]any)
					if !ok {
						return errors.New(fmt.Sprintf("数据格式错误: %+v", val)), reflect.Value{}
					}
					if err := fillSlice(newValueRes.Addr().Interface(), newSliceVal); err != nil {
						return err, reflect.Value{}
					}
				case reflect.Map:
					var newMapVal, ok = mapVal[key].(map[string]any)
					if !ok {
						return errors.New(fmt.Sprintf("数据格式错误: %+v", val)), reflect.Value{}
					}
					if err := fillMap(newValueRes.Addr().Interface(), newMapVal); err != nil {
						return err, reflect.Value{}
					}
				case reflect.Array:
					var newArrayVal, ok = mapVal[key].([]any)
					if !ok {
						return errors.New(fmt.Sprintf("数据格式错误: %+v", val)), reflect.Value{}
					}
					if err := fillArray(newValueRes.Addr().Interface(), newArrayVal); err != nil {
						return err, reflect.Value{}
					}
				case reflect.Struct:
					var newMapVal, ok = mapVal[key].(map[string]any)
					if !ok {
						return errors.New(fmt.Sprintf("数据格式错误: %+v", val)), reflect.Value{}
					}
					if err := fillStruct(newValueRes.Addr().Interface(), newMapVal); err != nil {
						return err, reflect.Value{}
					}
				default:
					return errors.New("不受支持的数据类型"), reflect.Value{}
				}

				return nil, newValueRes
			}

			switch value.Kind() {
			case reflect.Ptr:
				value = value.Elem()
			case reflect.Slice:
				fallthrough
			case reflect.Map:
				fallthrough
			case reflect.Array:
				fallthrough
			case reflect.Struct:
				var err, newValue = valueInterface()
				if err != nil {
					return err
				}
				value = newValue
			}

			newMap.SetMapIndex(mapKey, value)
		}

		return nil
	}

	for i, key := range keys {
		value := dataValue.MapIndex(key)

		var fieldName = value.Type().Field(i).Tag.Get("json")
		if fieldName == "" {
			fieldName = value.Type().Field(i).Name
		}
		var valueAddr = value.Addr().Interface()

		switch value.Kind() {
		case reflect.Struct:
			var newStruct, ok = mapVal[fieldName].(map[string]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式不正确：%s.(map[string]any)", fieldName))
			}
			if err := fillStruct(valueAddr, newStruct); err != nil {
				return err
			}
		case reflect.Slice:
			var newSlice, ok = mapVal[fieldName].([]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式不正确：%s.([]any)", fieldName))
			}
			if err := fillSlice(valueAddr, newSlice); err != nil {
				return err
			}
		case reflect.Array:
			var newArray, ok = mapVal[fieldName].([]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式错误：%s.([]any)", i))
			}
			if err := fillArray(valueAddr, newArray); err != nil {
				return err
			}
		case reflect.Map:
			var newMap, ok = mapVal[fieldName].(map[string]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式不正确：%s.(map[string]any)", fieldName))
			}
			if err := fillMap(valueAddr, newMap); err != nil {
				return err
			}
		default:
			var val, ok = mapVal[fieldName]
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式不正确：%s", fieldName))
			}
			if err := fillPrimitive(valueAddr, val); err != nil {
				return err
			}
		}
	}

	return nil
}

func fillArray(array interface{}, value []interface{}) error {
	var arrayValue = reflect.ValueOf(array).Elem()
	if len(value) < arrayValue.Len() {
		return errors.New("array out of bounds")
	}

	for i := 0; i < arrayValue.Len(); i++ {
		var itemValue = arrayValue.Index(i)
		var itemValAddr = itemValue.Addr().Interface()

		switch itemValue.Kind() {
		case reflect.Struct:
			var newStruct, ok = value[i].(map[string]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式不正确：%s.(map[string]any)", i))
			}
			if err := fillStruct(itemValAddr, newStruct); err != nil {
				return err
			}
		case reflect.Slice:
			var newSlice, ok = value[i].([]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式不正确：%s.([]any)", i))
			}
			if err := fillSlice(itemValAddr, newSlice); err != nil {
				return err
			}
		case reflect.Map:
			var newMap, ok = value[i].(map[string]any)
			if false == ok {
				return errors.New(fmt.Sprintf("数据格式不正确：%s.(map[string]any)", i))
			}
			if err := fillMap(itemValAddr, newMap); err != nil {
				return err
			}
		default:
			if err := fillPrimitive(itemValAddr, value[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func fillPrimitive(data, value interface{}) (err error) {
	var dataType = reflect.TypeOf(data).Elem()
	var dataValue = reflect.ValueOf(data).Elem()
	defer func() {
		if err01 := recover(); err01 != nil {
			err = fmt.Errorf("值类型不兼容: { %s: %v }, err: %v",
				dataType.Name(), reflect.ValueOf(value).Interface(), err01)
		}
	}()
	switch dataType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dataValue.SetInt(int64(reflect.ValueOf(value).Float()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		dataValue.SetUint(uint64(reflect.ValueOf(value).Float()))
	case reflect.Float32, reflect.Float64:
		dataValue.SetFloat(reflect.ValueOf(value).Float())
	case reflect.String:
		dataValue.SetString(reflect.ValueOf(value).String())
	case reflect.Bool:
		dataValue.SetBool(reflect.ValueOf(value).Bool())
	case reflect.Ptr:
		if !dataValue.IsNil() {
			dataValue.Set(reflect.New(dataValue.Type().Elem()))
		}
	case reflect.Array:
		dataValue.Set(reflect.ValueOf(value))
	default:
		err = fmt.Errorf("unsupported type: %v, %d", dataType, dataType.Kind())
	}

	return err
}

func (e *JsonToStruct) SetData(fn func(data *JsonData)) {
	e.lock.Lock()
	if fn != nil {
		fn(e.data)
	}
	e.lock.Unlock()
}

// Val 获取excel数据, 建议不要返回指针
func Val(fn func(val *JsonData) any) any {
	result.lock.RLock()
	defer result.lock.RUnlock()

	return fn(result.data)
}
