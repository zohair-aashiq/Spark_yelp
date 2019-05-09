package de.qimia.spark_yelp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class spark_yelp {

  def sampling(df1:DataFrame,df2:DataFrame,df3:DataFrame,df4:DataFrame,df5:DataFrame):DataFrame = {
    val df1_sample = df1.sample(false,0.01)
    val result = df1_sample.join(df2,"business_id")
      .join(df3,"business_id")
      .join(df5,"business_id")
    val result2 = result.join(df4,result.col("business_id") === df4.col("id"))
    val data = result2.sample(false,0.1)
    return data
  }

  def selectDistinctValues(df:DataFrame,columnName:String,fileName:String):Unit = {
    val df_distinct = df.select(columnName).distinct
    df_distinct
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("/opt/app/data/output/"+fileName+".csv")
  }

  def getJoinCount(dfAToJoin:DataFrame,dfBToJoin:DataFrame,joinByCol:String,countByCol:String,fileName:String):DataFrame = {
    // list business count for each category:
    val df1_df2_joined=dfAToJoin.join(dfBToJoin,joinByCol)
    val df = df1_df2_joined.groupBy(countByCol).count()
    df.write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("/opt/app/data/output/"+fileName+".csv")
    return df1_df2_joined
  }


  def getJoinCountAggr(df:DataFrame,joinByAggr:String,countByCol:String,fileName:String):Unit = {
    // list business count for each category:
    val df_A = df.groupBy(countByCol).agg(avg(joinByAggr))
    df_A
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("/opt/app/data/output/"+fileName+".csv")
  }
  def allCategoryCount(dfJoined:DataFrame,joinByColA:String,joinByColB:String,fileNameB:String){
    val df = dfJoined.groupBy(joinByColA).agg(count(joinByColB))
    /* df
       .coalesce(1)
       .write
       .format("com.databricks.spark.csv")
       .mode("overwrite")
       .option("header", "true")
       .save("/opt/app/data/output/"+fileNameB+".csv")
      */
  }

  def allCategory(dfJoined:DataFrame,dfToJoined:DataFrame,joinByColA:String,joinByColB:String,
                  countStringA:String,countStringB:String,fileNameA:String,fileNameB:String){
    val df = dfJoined.join(dfToJoined, dfJoined.col(joinByColA) === dfToJoined.col(joinByColB),"inner")
    allCategoryCount(df,countStringA,countStringB,fileNameB)

    /* df
      .repartition(1)
      .write.mode("overwrite")
      .option("header", "true")
      .save("/opt/app/data/output/"+fileNameA+".csv") */
  }
}
object spark_yelp  {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("yelp_data_set")
      .getOrCreate()

    val df_category = spark.read.format("csv").option("header", "true").load("/opt/app/data/category.csv")
    val df_user = spark.read.format("csv").option("header", "true").load("/opt/app/data/user.csv")
    val df_business = spark.read.format("csv").option("header", "true").load("/opt/app/data/business.csv")
    val df_review = spark.read.format("csv").option("header", "false").load("/opt/app/data/review.csv")
    val df_tip = spark.read.format("csv").option("header", "false").load("/opt/app/data/tip.csv")

    val df_user_col = df_user.withColumnRenamed("name", "user_name")
    val df_business_slted_cols = df_business.select("id","name","stars","review_count")
    val newBusinessNames = Seq("business_id", "business_name", "business_stars", "business_review_count")
    val newNamesReview = Seq("business_id","review_user_id","stars","date")
    val newNamesTip = Seq("user_id","business_id","date","likes")

    val df_tip_colName = df_tip.toDF(newNamesTip: _*).select("user_id","business_id","likes")
    val df_business_colName = df_business_slted_cols.toDF(newBusinessNames: _*)
    val df_category_colName=df_category.withColumnRenamed("id", "id_category")
    val df_review_colName = df_review.toDF(newNamesReview: _*).select("business_id","review_user_id","stars")
    val df_user_colName = df_user_col.select("id","user_name","review_count")

    val c = new spark_yelp
    val sampled_df = c.sampling(df_review_colName,df_tip_colName,df_category_colName,df_user_colName,df_business_colName)

    val category_df = sampled_df.select(df_category_colName.columns.map(col):_*)
    val business_df = sampled_df.select(df_business_colName.columns.map(col):_*)
    val tip_df = sampled_df.select(df_tip_colName.columns.map(col):_*)
    val review_df = sampled_df.select(df_review_colName.columns.map(col):_*)
    val user_df = sampled_df.select(df_user_colName.columns.map(col):_*)


    c.selectDistinctValues(category_df,"category","category_distinct")

    c.getJoinCount(category_df,business_df,"Business_id","category","business_count")

    val joined_catg_busn = c.getJoinCount(category_df,business_df,"Business_id","category","business_count")
    val joined_catg_rev = c.getJoinCount(category_df,review_df,"Business_id","category","review_count")
    val joined_catg_tip = c.getJoinCount(category_df,tip_df,"Business_id","category","tip_count")
    c.getJoinCountAggr(joined_catg_rev,"stars","category","average_review")
    c.getJoinCountAggr(joined_catg_tip,"likes","category","average_tip")

    c.allCategory(joined_catg_rev,user_df,"review_user_id","id","name","category","review_category","review_category_count")
    c.allCategory(joined_catg_tip,user_df,"user_id","id","name","category","tip_category","tip_category_count")
  }
}
