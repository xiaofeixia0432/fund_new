package router

import (
	"github.com/gin-gonic/gin"

	"go_echarts/controller"
)

func CreatRouters(router *gin.Engine) {
	router.GET("/GetDayFundInfo", controller.GetDayFundInfo)
	router.POST("/GetFundValue", controller.GetFundValue)
	router.GET("/GetFundInfo", controller.GetFundInfo)
	router.POST("/GetFundInfoByCodeName", controller.GetFundInfoByCodeName)

}
