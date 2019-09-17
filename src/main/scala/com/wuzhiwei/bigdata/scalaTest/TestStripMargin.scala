package com.wuzhiwei.bigdata.scalaTest

object TestStripMargin {


  val resultDf_word =
    """
      |  SELECT  lang,
      |  COUNT(1) AS pv,
      |  COUNT(DISTINCT did) AS uv,
      |
      |  COUNT(XXX) AS return_pv,
      |  COUNT(XXX) AS return_uv,
      |
      |  FROM resultDf
      |  where XXX >= 0 and len(XXX) > 0
      |
      |  GROUP BY XXX,XXX, lang
    """.stripMargin

  def main(args: Array[String]): Unit = {

    print(resultDf_word)
  }





}
