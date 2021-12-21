package main

import (
	"errors"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/go-xorm/xorm"
	"github.com/gocolly/colly/v2"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var DbEngin *xorm.Engine

type Port struct {
	Id          int64  `xorm:"pk autoincr int(11)" json:"id"`
	Code        string `xorm:"varchar(128)"  json:"code"`       //港口代码
	FullName    string `xorm:"varchar(256)"  json:"fullName"`   //港口全称，中英文名
	CnName      string `xorm:"varchar(256)"  json:"cnName"`     //港口中文名
	EnName      string `xorm:"varchar(256)"  json:"enName"`     //港口英文名
	CnCountry   string `xorm:"varchar(256)" json:"cnCountry"`   //所在国家中文名
	EnCountry   string `xorm:"varchar(256)" json:"enCountry"`   //所在国家英文名
	CountryCode string `xorm:"varchar(256)" json:"countryCode"` //国家代码
	Route       string `xorm:"varchar(256)" json:"route"`       //航线
}

type Pages struct {
	Id  int64  `xorm:"pk autoincr int(11)" json:"id"`
	Url string `xorm:"varchar(128)"  json:"url"`
}

func init() {
	driverName := "sqlite3"
	dsName := "./port1220.db"
	err := errors.New("")
	DbEngin, err = xorm.NewEngine(driverName, dsName)
	if nil != err && "" != err.Error() {
		panic(err)
	}
	//是否显示SQL语句
	DbEngin.ShowSQL(true)
	//数据库最大打开的连接数
	DbEngin.SetMaxOpenConns(10)

	//自动创建表
	err = DbEngin.Sync2(new(Port), new(Pages))
	if err != nil {
		panic(err)
	}
	err = DbEngin.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("init database success!")
}

var userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
var baseURL = "https://www.sofreight.com/ports"

func grabPage(route string) {
	c := colly.NewCollector(colly.UserAgent(userAgent))
	c.WithTransport(&http.Transport{DisableKeepAlives: true})
	c.OnHTML("#portlist", func(e *colly.HTMLElement) {
		e.DOM.Find("table").Find("tbody").Find("tr").
			Find("td").Find("a").Each(func(i int, d *goquery.Selection) {
			href, _ := d.Attr("href")
			portChan <- href
		})
	})
	e := c.Visit(route)
	if e != nil {
		log.Fatalln(e)
	}
}

func allPage() {
	c := colly.NewCollector(colly.UserAgent(userAgent))
	c.WithTransport(&http.Transport{DisableKeepAlives: true})
	c.OnHTML("#lineList", func(e *colly.HTMLElement) {
		e.DOM.Find("a").Each(func(i int, d *goquery.Selection) {
			href, _ := d.Attr("href")
			s, _ := d.Html()
			re := regexp.MustCompile("[0-9]+")
			nums, _ := strconv.Atoi(re.FindAllString(s, -1)[0])
			totalpages := int(math.Ceil(float64(nums) / float64(50)))
			for i := 1; i <= totalpages; i++ {
				var page = Pages{}
				page.Url = href + "?page=" + strconv.Itoa(i)
				_, _ = DbEngin.InsertOne(page)
			}
		})
	})
	e := c.Visit(baseURL)
	if e != nil {
		panic(e)
	}
}

func format(name string) (string, string) {
	name = strings.ToUpper(name)
	r := strings.Index(name, " ")
	f := name[:r]
	l := name[r:]
	return f, strings.Trim(l, " ")
}

func grabPort(portURL string) {
	c := colly.NewCollector(colly.UserAgent(userAgent))
	c.WithTransport(&http.Transport{DisableKeepAlives: true})
	c.OnHTML("#portMain", func(e *colly.HTMLElement) {
		e.DOM.Find("table").Find("tbody").Each(func(i int, tr *goquery.Selection) {
			tds := tr.Find("td[class!=label]")
			var port = Port{}
			tds.Each(func(i int, d *goquery.Selection) {
				if i == 0 {
					//港口名称
					fullName := d.Text()
					port.FullName = fullName
					f, l := format(fullName)
					port.CnName = f
					port.EnName = l
				} else if i == 1 {
					//港口代码
					code := d.Text()
					port.Code = strings.ToUpper(code)
				} else if i == 3 {
					//国家代码
					countryCode := d.Text()
					port.CountryCode = strings.ToUpper(countryCode)
				} else if i == 4 {
					//国家
					cnCountry := d.Text()
					port.CnCountry = cnCountry
				} else if i == 5 {
					//国家
					enCountry := d.Text()
					port.EnCountry = strings.ToUpper(enCountry)
				} else if i == 6 {
					//国家
					route := d.Text()
					port.Route = route
				}
			})
			insertChan <- port
			//_, _ = DbEngin.InsertOne(port)
		})
	})
	e := c.Visit(portURL)
	if e != nil {
		fmt.Println(e)
	}
}

func insertPort(port Port) {
	_, _ = DbEngin.InsertOne(port)
}

var portChan = make(chan string, 1000)
var insertChan = make(chan Port, 1000)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	//go allPage()
	go func() {
		pg := make([]Pages, 0)
		_ = DbEngin.Find(&pg)
		for _, u := range pg {
			grabPage(u.Url)
		}
	}()
	for {
		select {
		case p := <-portChan:
			grabPort(p)
		case i := <-insertChan:
			go insertPort(i)
		case <-time.After(time.Minute * 100):
			fmt.Println("超时退出...")
			os.Exit(1)
		}
	}
}
