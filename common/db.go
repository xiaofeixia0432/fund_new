package common

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

const (
	DBName    = "fund"
	DBHost    = "127.0.0.1:3306"
	DBUser    = "root"
	DBPasswd  = "111111"
	DBCodeset = "utf8"

	MongoDBUser         = "test1Fund"
	MongoDBPwd          = "123"
	MongoDBHost         = "127.0.0.1:27017"
	MongoDBDataBaseName = "test1Fund"

	MongoTimeout = 3 * time.Second
)

var DB *gorm.DB
var MongoClient *mongo.Client

func InitDB() error {
	var err error
	DB, err = gorm.Open(mysql.New(mysql.Config{
		DriverName: "mysql",
		DSN:        fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s", DBUser, DBPasswd, DBHost, DBName, DBCodeset),
	}), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
	})
	if err != nil {
		fmt.Printf("gorm open is error, err: %v\n", err)
		return err
	}
	db, err := DB.DB()
	if err != nil {
		return err
	}
	db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(100)
	return nil
}

func ConnectToDB(uri string, timeout time.Duration) error {
	var err error
	// 设置连接超时时间
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// 通过传进来的uri连接相关的配置
	o := options.Client().ApplyURI(uri)
	// 发起链接
	MongoClient, err = mongo.Connect(ctx, o)
	if err != nil {
		fmt.Println(err)
		return err
	}
	// fmt.Println(o)
	// 判断服务是不是可用
	if err = MongoClient.Ping(context.Background(), readpref.Primary()); err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println("mongodb ping is success!")
	return nil
}

func InitMongoDB() error {
	mongoConStr := fmt.Sprintf("mongodb://%s:%s@%s", MongoDBUser, MongoDBPwd, MongoDBHost)
	err := ConnectToDB(mongoConStr, MongoTimeout)
	if err != nil {
		return err
	}
	return nil
}
