package main

import (
    "fmt"
    "os"
    "flag"
    //"bufio"
    "encoding/csv"
    "strings"
    "time"
    "strconv"
	//"sort"
    "io"
    "github.com/axgle/mahonia"
    /*"github.com/parnurzeal/gorequest"
    "encoding/json"*/
    //"os/exec"
    //"sync"
    //"runtime"
)
var G_m_Center = map[string]int {
    "上海":0,
    "成都":0,
    "西安":0,
    "沈阳":0,
    "广州":0,
    "济南":0,
    "合肥":0,
}
var G_m_Level = map[string]int {
    "座席":0,
    "坐席":0,
}
var G_m_Department = map[string]int{
    //"测试": 0,
    //"营业": 0,
    //"直属": 0,
    "新兵": 0,
    "互联网": 0,
}

var TimeLayout = "2006/1/2"
var EmployeeRet = []string{"UM账号", "分中心", "区", "部名称", "组名称", "实属营业部", "实属营业组", "客户经理生效时间"}
var EarningRet = []string{"座席UM", "分中心", "区", "部", "组", "名单类型", "折标系数", "实收件(承保)", "实收年度化规模保费(承保)", "折标保费"}
type EmployeeProperty struct {
    Center                              string
    Department                          string
    GroupName                           string
    UMID                                string
    Region                              string
    RealDepartment                      string
    RealGroup                           string
    ConvertTime                         string
    ConvertDuration                     int64
    OldRegion                           string
}
var G_m_PropertyIndex =map[string]int {
    "Level": -1,
    "Center": -1,
    "Department": -1,
    "GroupName": -1,
    "UMID": -1,
    "OldRegion": -1,
    "RealDepartment": -1,
    "RealGroup": -1,
    "ConvertTime": -1,
}
var G_m_EarningIndex =map[string]int {
    "UMID": -1,
    "Center": -1,
    "ListType": -1,
    "RealUnit": -1,
    "RealCost": -1,
}
type EarningProperty struct {
    UMID                        string
    Center                      string
    Region                      string
    Department                  string
    Group                       string
    ListType                    string
    Ratio                       string
    RealUnit                    string
    RealCost                    string
    DiscountCost                string
}

var G_m_DiscountRatio = map[string]int {
    "ListType": -1,
    "Ratio": -1,
}
type EarningInfos []EarningProperty
type EmployeeInfos []EmployeeProperty
func (c EmployeeInfos) Len() int {
	return len(c)
}
func (c EmployeeInfos) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
func (c EmployeeInfos) Less(i, j int) bool {
	return c[i].ConvertDuration > c[j].ConvertDuration
	//return c[i].SubmitTime < c[j].SubmitTime
}

func CheckExist(m_src map[string]int, key string, equal bool) bool {
    //fmt.Printf("key:%s\n", key)
    //fmt.Println(m_src)
    if equal {
        if _, ok := m_src[key]; ok {
            //fmt.Println("check true ++++++++++++")
            return true
        } else {
            //fmt.Println("check false ++++++++++")
            return false
        }
    } else {
        for k, _ := range m_src {
            if strings.Contains(key, k) {
                //fmt.Println("check false -------")
                return false
            }
        }
        //fmt.Println("check true -------")
        return true
    }

    return true
}
func SubTime(begin, end string) (int64) {
    loc, _ := time.LoadLocation("Local")
    t_begin, _ := time.ParseInLocation(TimeLayout, begin, loc)
    t_end, _ := time.ParseInLocation(TimeLayout, end, loc)

    d_ret := t_end.Sub(t_begin)

    //4380
    return (int64)(d_ret.Hours() / 24)
}
func WriteCsvFile(file_name string, values []string) error {
    f,err := os.OpenFile(file_name, os.O_CREATE | os.O_APPEND | os.O_RDWR, 0660)
    if(err != nil){
        panic(err)
    }
	defer f.Close()
    encoder := mahonia.NewEncoder("gbk")
    w := csv.NewWriter(encoder.NewWriter(f))

    w.Write(values)
    w.Flush()
    return nil
}
func WriteAllCsvValues(file_name string, infos EmployeeInfos) error {
    f,err := os.OpenFile(file_name, os.O_CREATE | os.O_APPEND | os.O_RDWR, 0660)
    if(err != nil){
        panic(err)
    }
	defer f.Close()
    encoder := mahonia.NewEncoder("gbk")
    w := csv.NewWriter(encoder.NewWriter(f))
    for _, info := range infos {
        app_ret_arr := make([]string, 0)
        app_ret_arr = append(app_ret_arr, info.UMID, info.Center, info.Region, info.Department, info.GroupName, info.RealDepartment, info.RealGroup, info.ConvertTime)
        w.Write(app_ret_arr)
        w.Flush()
    }

    return nil
}
func WriteAllCsvEarning(file_name string, infos EarningInfos) error {
    f,err := os.OpenFile(file_name, os.O_CREATE | os.O_APPEND | os.O_RDWR, 0660)
    if(err != nil){
        panic(err)
    }
	defer f.Close()
    encoder := mahonia.NewEncoder("gbk")
    w := csv.NewWriter(encoder.NewWriter(f))
    for _, info := range infos {
        app_ret_arr := make([]string, 0)
        app_ret_arr = append(app_ret_arr, info.UMID, info.Center, info.Region, info.Department, info.Group, info.ListType, info.Ratio, info.RealUnit, info.RealCost, info.DiscountCost)
        w.Write(app_ret_arr)
        w.Flush()
    }

    return nil
}
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}


func ParseEmployeeInfo(file1, earning_path, ratio_path string) error {
    f,err := os.Open(file1)
    if(err != nil){
        panic(err)
    }
	defer f.Close()
    decoder := mahonia.NewDecoder("gbk")
    reader := csv.NewReader(decoder.NewReader(f))
    b_firstline := true

    var E_tmp EmployeeInfos
    m_Find_Region := make(map[string]string)
    m_Find_All_ByUM := make(map[string]*EmployeeProperty)
    num_t:=0
    for {
        record, err := reader.Read()
        if num_t <= 5 {
            num_t += 1
            fmt.Println("------------------=")
            fmt.Println(record)
            //time.Sleep(3 * time.Second)
        }
        if err == io.EOF {
            fmt.Println("finish")
            break
        } else if err != nil {
            fmt.Println("Error:", err)
            return nil
        }
        if b_firstline{
            for ind, label := range record {
                if label == "职等" {
                    G_m_PropertyIndex["Level"] = ind
                } else if label == "分中心" {
                    G_m_PropertyIndex["Center"] = ind
                } else if label == "部名称" {
                    G_m_PropertyIndex["Department"] = ind
                } else if label == "组名称" {
                    G_m_PropertyIndex["GroupName"] = ind
                } else if strings.EqualFold(label, "UM账号") {
                    G_m_PropertyIndex["UMID"] = ind
                //} else if strings.HasPrefix(label, "UM") {
                /*} else if strings.HasPrefix(label, "UM") {
                    subUm := strings.TrimPrefix(label, "UM")
                    if subUm == "账号" {
                        G_m_PropertyIndex["UMID"] = ind
                    }*/
                    /*umstr := []rune(label)
                    if umstr[2] == "账" && umstr[3] == "号" {
                        G_m_PropertyIndex["UMID"] = ind
                    }*/
                } else if label == "区" {
                    G_m_PropertyIndex["OldRegion"] = ind
                } else if label == "实属营业部" {
                    G_m_PropertyIndex["RealDepartment"] = ind
                } else if label == "实属营业组" {
                    G_m_PropertyIndex["RealGroup"] = ind
                } else if label == "客户经理生效时间" {
                    G_m_PropertyIndex["ConvertTime"] = ind
                }
            }
            b_firstline = false
            for k, v := range G_m_PropertyIndex {
                fmt.Printf("%s:%d\n", k, v)
            }
            continue
        }

        if CheckExist(G_m_Center, record[G_m_PropertyIndex["Center"]], true) && CheckExist(G_m_Level, record[G_m_PropertyIndex["Level"]], true) && CheckExist(G_m_Department, record[G_m_PropertyIndex["Department"]], false) {
            now_str := time.Now().Format(TimeLayout)
            t_days := SubTime(record[G_m_PropertyIndex["ConvertTime"]], now_str)
            fmt.Printf("convert:%s, now:%s, days:%d\n", record[G_m_PropertyIndex["ConvertTime"]], now_str, t_days)
            if t_days < 183 || record[G_m_PropertyIndex["OldRegion"]] == "" {
                fmt.Printf("t_days:%d, region:%s\n", t_days, record[G_m_PropertyIndex["OldRegion"]])
                continue
            }
            e_obj := new(EmployeeProperty)
            e_obj.Center = record[G_m_PropertyIndex["Center"]]
            e_obj.Department = record[G_m_PropertyIndex["Department"]]
            e_obj.GroupName = record[G_m_PropertyIndex["GroupName"]]
            e_obj.UMID = record[G_m_PropertyIndex["UMID"]]
            e_obj.RealDepartment = record[G_m_PropertyIndex["RealDepartment"]]
            e_obj.RealGroup = record[G_m_PropertyIndex["RealGroup"]]
            e_obj.ConvertTime = record[G_m_PropertyIndex["ConvertTime"]]
            e_obj.ConvertDuration = t_days
            e_obj.OldRegion = record[G_m_PropertyIndex["OldRegion"]]
            if num_t <= 5 {
                fmt.Println("Center, Department, GroupName, UMID, RealDepartment, RealGroup, ConvertTime, ConvertDuration, OldRegion")
                fmt.Println(*e_obj)
            }

            E_tmp = append(E_tmp, *e_obj)

            m_Find_Region[e_obj.Center+e_obj.RealDepartment] = e_obj.OldRegion
        }
    }
    var E_ret EmployeeInfos
    fmt.Println(E_tmp)
    for _, e_obj_f := range E_tmp {
        new_obj := e_obj_f
        if v, ok := m_Find_Region[e_obj_f.Center + e_obj_f.Department]; ok {
            if v == "" {
                new_obj.Region = "总计"
            } else {
                new_obj.Region = v
            }
        } else {
            new_obj.Region = new_obj.OldRegion
            new_obj.Department = new_obj.RealDepartment
            new_obj.GroupName = new_obj.RealGroup
        }
        m_Find_All_ByUM[new_obj.UMID] = &new_obj
        E_ret = append(E_ret, new_obj)
    }
    //fmt.Println(E_ret)

    WriteCsvFile("./target1.csv", EmployeeRet)
    WriteAllCsvValues("./target1.csv", E_ret)
    ParseRealEarning(m_Find_All_ByUM, earning_path, ratio_path)

    return nil
}

func ParseRealEarning(m_src map[string]*EmployeeProperty, earning_path, ratio_path string) error {
    m_ratios := make(map[string]string)
    f1,err := os.Open(ratio_path)
    if(err != nil){
        panic(err)
    }
	defer f1.Close()
    decoder1 := mahonia.NewDecoder("gbk")
    reader1 := csv.NewReader(decoder1.NewReader(f1))
    b_firstline1 := true
    for {
        record1, err := reader1.Read()
        if err == io.EOF {
            fmt.Println("finish")
            break
        } else if err != nil {
            fmt.Println("Error:", err)
            return nil
        }
        if b_firstline1{
            for ind, label := range record1 {
                if label == "名单批次名称" {
                    G_m_DiscountRatio["ListType"] = ind
                } else if label == "名单折标系数" {
                    G_m_DiscountRatio["Ratio"] = ind
                }
            }
            b_firstline1 = false
            continue
        }
        m_ratios[record1[G_m_DiscountRatio["ListType"]]]= record1[G_m_DiscountRatio["Ratio"]]
    }


    f,err := os.Open(earning_path)
    if(err != nil){
        panic(err)
    }
	defer f.Close()
    decoder := mahonia.NewDecoder("gbk")
    reader := csv.NewReader(decoder.NewReader(f))
    b_firstline := true
    var E_tmp EarningInfos
    num_t := 0
    for {
        record, err := reader.Read()
        if num_t <= 5 {
            num_t += 1
            fmt.Println(record)
            fmt.Println("------------------=")
            //time.Sleep(3 * time.Second)
        }
        if err == io.EOF {
            fmt.Println("finish")
            break
        } else if err != nil {
            fmt.Println("Error:", err)
            return nil
        }
        if b_firstline{
            for ind, label := range record {
                //if label == "座席UM" {
                if strings.HasPrefix(label,  "座席") && strings.HasSuffix(label, "UM") {
                    G_m_EarningIndex["UMID"] = ind
                } else if label == "分中心" {
                    G_m_EarningIndex["Center"] = ind
                } else if label == "名单类型" {
                    G_m_EarningIndex["ListType"] = ind
                //} else if label == "实收件(承保)" {
                } else if strings.HasPrefix(label, "实收件") {
                    G_m_EarningIndex["RealUnit"] = ind
                //} else if label == "实收年化规模保费(承保)" {
                } else if strings.HasPrefix(label, "实收年化规模保费") {
                    G_m_EarningIndex["RealCost"] = ind
                }
            }
            b_firstline = false
            for k, v := range G_m_EarningIndex {
                fmt.Printf("%s:%d\n", k, v)
            }
            continue
        }

        if CheckExist(G_m_Center, record[G_m_EarningIndex["Center"]], true) {
            if t_obj, ok := m_src[record[G_m_EarningIndex["UMID"]]]; ok {
                e_obj := new(EarningProperty)
                e_obj.UMID = record[G_m_EarningIndex["UMID"]]
                e_obj.Center = record[G_m_EarningIndex["Center"]]
                e_obj.Region = t_obj.Region
                e_obj.Department = t_obj.Department
                e_obj.Group = t_obj.GroupName
                e_obj.ListType = record[G_m_EarningIndex["ListType"]]
                e_obj.Ratio = m_ratios[e_obj.ListType]
                e_obj.RealUnit = record[G_m_EarningIndex["RealUnit"]]
                e_obj.RealCost = record[G_m_EarningIndex["RealCost"]]


                t_cost, _ := strconv.ParseFloat(e_obj.RealCost, 64)
                t_ratio, _ := strconv.ParseFloat(e_obj.Ratio, 64)
                t_dc := t_cost * t_ratio
                e_obj.DiscountCost = strconv.FormatFloat(t_dc, 'f', -1, 64)

                E_tmp = append(E_tmp, *e_obj)
            }

        }
    }
    WriteCsvFile("./target2.csv", EarningRet)
    WriteAllCsvEarning("./target2.csv", E_tmp)

    return nil
}
func main(){
    file1 := flag.String("file1", "寿险在职人员清单.csv", "path")
    earning_path := flag.String("file4", "全渠道实收保费统计月回算日报.csv", "path")
    ratio_path := flag.String("file5", "折标系数.csv", "path")
    flag.Parse()
    ParseEmployeeInfo(*file1, *earning_path, *ratio_path)
    //time.Sleep(30 * time.Second)
}
