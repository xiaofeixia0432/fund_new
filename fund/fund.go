package fund

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go_echarts/common"
)

type Fund struct {
	ID                int     // 基金序号
	Code              string  `json:"code"`                 // 基金代码
	Name              string  `json:"name"`                 // 基金简称
	Date              string  `json:"date"`                 // 日期
	UnitValue         float64 `json:"unit_value"`           // 单位净值
	TotalValue        float64 `json:"total_value"`          // 累计净值
	DaySwellRate      string  `json:"day_swell_rate"`       // 日增长率
	WeekSwellRate     string  `json:"week_swell_rate"`      // 近一周增长率
	MothSwellRate     string  `json:"moth_swell_rate"`      // 近一个月增长率
	TreeMothSwellRate string  `json:"tree_moth_swell_rate"` // 近三个月增长率
	SixMothSwellRate  string  `json:"six_moth_swell_rate"`  // 近六个月增长率
	YearSwellRate     string  `json:"year_swell_rate"`      // 近一年增长率
	TwoYearSwellRate  string  `json:"two_year_swell_rate"`  // 近两年增长率
	TreeYearSwellRate string  `json:"tree_year_swell_rate"` // 近三年增长率
	ThisYearSwellRate string  `json:"this_year_swell_rate"` // 近年来增长率
	CreateSwellRate   string  `json:"create_swell_rate"`    // 成立以来
	CustomRate        string  `json:"custom_rate"`          // 自定义
	Fee               string  `json:"fee"`                  // 手续费
	IsBuy             bool    `json:"is_buy"`               // 是否可以购买
}

type ListCodeMsg struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

type DateValue struct {
	Value float64 `json:"unitvalue" bson:"unitvalue" gorm:"unit_value"`
	Date  string  `json:"date" bson:"date" gorm:"date"`
}

func CreateExcel(file string) *os.File {
	f, err := os.Create(file)
	if err != nil {
		fmt.Printf("create excel file is error, err: %v\n", err)
		return nil
	}
	return f
}

func WriteExcelHeader(s *os.File) {
	header := []string{"基金序号", "基金代码", "基金简称", "日期", "单位净值", "累计净值",
		"日增长率", "近一周增长率", "近一个月增长率", "近三个月增长率", "近六个月增长率",
		"近一年增长率", "近两年增长率", "近三年增长率", "近年来增长率", "成立以来",
		"自定义", "手续费", "是否可以购买"}
	_, err := s.WriteString("\xEF\xBB\xBF") // 写入UTF-8 BOM
	if err != nil {
		fmt.Printf("writeExcelHeader is error, err: %v\n", err)
		return
	}
	w := csv.NewWriter(s)
	err = w.Write(header)
	if err != nil {
		fmt.Printf("writeExcelHeader is error, err: %v\n", err)
		return
	}
	w.Flush()
}

func CloseFile(s *os.File) {
	s.Close()
}

func InsertFund(s *os.File, fund *Fund) {
	strconv.FormatFloat(fund.TotalValue, 'E', -1, 64)
	data := []string{strconv.Itoa(fund.ID), fund.Code, fund.Name, fund.Date, strconv.FormatFloat(fund.UnitValue,
		'E', -1, 64),
		strconv.FormatFloat(fund.TotalValue, 'E', -1, 64), fund.DaySwellRate, fund.WeekSwellRate,
		fund.MothSwellRate, fund.TreeMothSwellRate, fund.SixMothSwellRate, fund.YearSwellRate, fund.TwoYearSwellRate,
		fund.TreeYearSwellRate, fund.ThisYearSwellRate, fund.CreateSwellRate, fund.CustomRate, fund.Fee, "Y"}
	w := csv.NewWriter(s)
	w.Write(data)
	w.Flush()
}

func StoreFund(f *Fund) error {
	// 未使用gorm
	sql := "insert into fund(code,name, date,unit_value ,total_value ,dayswell_rate,weekswell_rate ,monthswell_rate ,threemonthswell_rate ,sixmonthswell_rate,yearswell_rate,twoyearswell_rate ,threeyearswell_rate ,thisyearwell_rate ,createswell_rate ,custom_rate ,fee, isbuy) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	db, err := common.DB.DB()
	stmt, err := db.Prepare(sql)
	defer stmt.Close()
	if err != nil {
		fmt.Printf("err: %v", err)
		return err
	}
	_, err = stmt.Exec(f.Code, f.Name, f.Date, f.UnitValue, f.TotalValue, f.DaySwellRate, f.WeekSwellRate,
		f.MothSwellRate, f.TreeMothSwellRate, f.SixMothSwellRate, f.YearSwellRate, f.TwoYearSwellRate,
		f.TreeYearSwellRate, f.ThisYearSwellRate, f.CreateSwellRate, f.CustomRate, f.Fee, f.IsBuy)
	if err != nil {
		return err
	}
	return nil
}

func CountPage(url string) (itemnum, pagenum int, err error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET",
		url,
		nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set(common.HeaderCookieKey, common.HeaderCookieValue)
	req.Header.Set(common.HeaderContentTypeKey, common.HeaderContentTypeValue)
	req.Header.Set(common.HeaderHostKey, common.HeaderHostValue)
	req.Header.Set(common.HeaderRefererKey, common.HeaderRefererValue)
	req.Header.Set(common.HeaderUserAgentKey, common.HeaderUserAgentValue)
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}
	// var i map[string]interface{}
	pageFirstIndex := bytes.Index(body, []byte("pageNum:"))
	pageLastIndex := bytes.LastIndex(body, []byte(",allPages"))
	firstIndex := bytes.Index(body, []byte("allPages:"))
	lastIndex := bytes.LastIndex(body, []byte(",allNum"))
	fmt.Println(string(body))
	page, err := strconv.Atoi(string(body[firstIndex+9 : lastIndex]))
	item, err := strconv.Atoi(string(body[pageFirstIndex+8 : pageLastIndex]))
	fmt.Printf("page:%v, totalpage:%v\n", item, page)
	return item, page, nil
}

func SelectFundCodeName() ([]ListCodeMsg, error) {
	listCodeMsgSlice := make([]ListCodeMsg, 0)
	db, err := common.DB.DB()
	rows, err := db.Query("select distinct code,name from fund")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		codeMsg := ListCodeMsg{}
		if err = rows.Scan(&codeMsg.Code, &codeMsg.Name); err != nil {
			return nil, err
		}
		listCodeMsgSlice = append(listCodeMsgSlice, codeMsg)
	}
	return listCodeMsgSlice, nil
}

func SelectFundInfoByCodeName(code, name string) ([]Fund, error) {
	fundSlice := make([]Fund, 0)
	db, err := common.DB.DB()
	sqlStr := fmt.Sprintf("select id,code,name, date,unit_value ,total_value ,dayswell_rate,weekswell_rate ,monthswell_rate ,threemonthswell_rate ,sixmonthswell_rate,yearswell_rate,twoyearswell_rate ,threeyearswell_rate ,thisyearwell_rate ,createswell_rate ,custom_rate ,fee, isbuy from fund where code = '%s' and name = '%s'", code, name)
	rows, err := db.Query(sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		f := Fund{}
		if err = rows.Scan(&f.ID, &f.Code, &f.Name, &f.Date, &f.UnitValue, &f.TotalValue, &f.DaySwellRate, &f.WeekSwellRate,
			&f.MothSwellRate, &f.TreeMothSwellRate, &f.SixMothSwellRate, &f.YearSwellRate, &f.TwoYearSwellRate,
			&f.TreeYearSwellRate, &f.ThisYearSwellRate, &f.CreateSwellRate, &f.CustomRate, &f.Fee, &f.IsBuy); err != nil {
			return nil, err
		}
		fundSlice = append(fundSlice, f)
	}
	return fundSlice, nil
}

func (f *Fund) StoreMongoDB() error {
	c := common.MongoClient.Database(common.MongoDBDataBaseName).Collection("fundinfo")
	fmt.Println(c)
	res, err := c.InsertOne(context.Background(), &f)
	if err != nil {
		return err
	}
	fmt.Printf("res: %v\n", res.InsertedID)
	return nil
}

func StoreFundData(url string, s *os.File, items, pages int) error {
	client := &http.Client{}
	req, err := http.NewRequest("GET",
		url,
		nil)
	if err != nil {
		return err
	}
	req.Header.Set(common.HeaderCookieKey, common.HeaderCookieValue)
	req.Header.Set(common.HeaderContentTypeKey, common.HeaderContentTypeValue)
	req.Header.Set(common.HeaderHostKey, common.HeaderHostValue)
	req.Header.Set(common.HeaderRefererKey, common.HeaderRefererValue)
	req.Header.Set(common.HeaderUserAgentKey, common.HeaderUserAgentValue)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var i interface{}
	firstIndex := bytes.Index(body, []byte{'['})
	lastIndex := bytes.LastIndex(body, []byte{']'})

	data := body[firstIndex : lastIndex+1]
	err = json.Unmarshal(data, &i)
	if err != nil {
		fmt.Printf("json unmarshal is error, err:%v\n", err)
		return err
	}
	// fmt.Printf("i: %v\n", i)
	// b := re.FindAllStringSubmatch(string(body), -1)
	// 先进行一次断言判断是否接口数组
	content := i.([]interface{})

	// fmt.Println(len(content))
	// fmt.Println(reflect.TypeOf(i))
	// fmt.Println(reflect.TypeOf(content[0]), reflect.TypeOf(content[1]))
	for index, value := range content {
		// 再次断言具体类型
		fundData, ok := value.(string)
		if ok {
			msg := strings.Split(fundData, ",")
			fund := &Fund{}
			fund.ID = items*(pages-1) + index + 1
			fund.Code = msg[0]
			fund.Name = msg[1]
			fund.Date = msg[3]
			fund.UnitValue, err = strconv.ParseFloat(msg[4], 64)
			if err != nil {
				fmt.Printf("unit value parsefloat error, err:%v\n", err)
				fund.UnitValue = 0.0
			}
			fund.TotalValue, err = strconv.ParseFloat(msg[5], 64)
			if err != nil {
				fmt.Printf("total value parsefloat error, err:%v\n", err)
				fund.TotalValue = 0.0
			}
			fund.DaySwellRate = msg[6]
			fund.WeekSwellRate = msg[7]
			fund.MothSwellRate = msg[8]
			fund.TreeMothSwellRate = msg[9]
			fund.SixMothSwellRate = msg[10]
			fund.YearSwellRate = msg[11]
			fund.TwoYearSwellRate = msg[12]
			fund.TreeYearSwellRate = msg[13]
			fund.ThisYearSwellRate = msg[14]
			fund.CreateSwellRate = msg[15]
			fund.CustomRate = msg[18]
			fund.Fee = msg[20]
			fund.IsBuy = true
			// 插入csv文件
			InsertFund(s, fund)

			currentTime := time.Now()
			preTime := currentTime.AddDate(0, 0, -1)
			fmt.Printf("current time: %v, preTime: %v\n", currentTime, preTime)
			updateMysqlDate := fmt.Sprintf("%04d-%02d-%02d 00:00:00", preTime.Year(), preTime.Month(), preTime.Day())
			//updateMongoDate := fmt.Sprintf("%04d-%02d-%02d", preTime.Year(), preTime.Month(), preTime.Day())
			InsertMysqlFlag, err := IsInsertMysql(fund.Code, updateMysqlDate)
			if err != nil {
				fmt.Printf("err: %v\n", err)
				return err
			}
			//InsertMongoDBFlag, err := IsInsertMongoDB(fund.Code, updateMongoDate)
			//if err != nil {
			//	fmt.Printf("err: %v\n", err)
			//	return err
			//}
			if InsertMysqlFlag {
				// 插入数据库中
				err = StoreFund(fund)
				if err != nil {
					return err
				}
			} else {
				// 直接退出循环,防止重复操作造成数据重复
				break
			}
			//if InsertMongoDBFlag {
			//	// 插入mongodb
			//	err = fund.StoreMongoDB()
			//	if err != nil {
			//		fmt.Printf("store mongodb is error, err: %v\n", err)
			//		return err
			//	}
			//} else {
			//	// 直接退出循环,防止重复操作造成数据重复
			//	break
			//}
		}
	}
	return nil
}

// SelectValue 获取净值
func SelectValue(code string) ([]DateValue, error) {
	c := common.MongoClient.Database(common.MongoDBDataBaseName).Collection("fundinfo")
	fileter := bson.D{{"code", code}}
	projection := bson.D{
		{"unitvalue", 1},
		{"date", 1},
	}
	cursors, err := c.Find(context.Background(), fileter, options.Find().SetProjection(projection))
	if err != nil {
		return nil, err
	}

	var res []DateValue
	err = cursors.All(context.Background(), &res)
	if err != nil {
		return nil, err
	}
	fmt.Printf("res: %v\n", res)
	return res, nil
}

func GetFundValueFromMySQL(code string) ([]DateValue, error) {
	dataValues := make([]DateValue, 0)
	db, err := common.DB.DB()
	sqlstr := fmt.Sprintf("select distinct date,unit_value from fund where code = '%s'", code)
	rows, err := db.Query(sqlstr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var d DateValue
		if err = rows.Scan(&d.Date, &d.Value); err != nil {
			return nil, err
		}
		dataValues = append(dataValues, d)
	}
	return dataValues, nil
}

func IsInsertMysql(code, date string) (bool, error) {
	db, err := common.DB.DB()
	var count int
	sqlstr := fmt.Sprintf("select count(*) from fund where date = '%s' and code = '%s'", date, code)
	err = db.QueryRow(sqlstr).Scan(&count)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return true, nil
	} else {
		return false, nil
	}
}

func IsInsertMongoDB(code, date string) (bool, error) {
	c := common.MongoClient.Database(common.MongoDBDataBaseName).Collection("fundinfo")
	fileter := bson.D{{"date", date}, {"code", code}}
	count, err := c.CountDocuments(context.Background(), fileter)
	if err != nil {
		return false, err
	}
	fmt.Printf("count: %v\n", count)
	if count == 0 {
		return true, nil
	} else {
		return false, nil
	}

}
