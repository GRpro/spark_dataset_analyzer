import com.gr.analytics.{Application, ArgParser}
import org.scalatest.WordSpec

import scala.util.Try

class ApplicationSpec extends WordSpec {

  private val testArgs =
    s"""
      |{
      |   "sourcePath": "${getFileWithUtil("sample.csv")}",
      |   "destPath": "target/output/",
      |   "csvOptions": {
      |     "ignoreLeadingWhiteSpace": "true",
      |     "ignoreTrailingWhiteSpace": "true",
      |     "quote": "'"
      |   },
      |   "conversions": [
      |     {
      |       "existing_col_name": "name",
      |       "new_col_name": "first_name",
      |       "new_data_type": "string"
      |     },
      |     {
      |       "existing_col_name": "age",
      |       "new_col_name": "total_years",
      |       "new_data_type": "integer"
      |     },
      |     {
      |       "existing_col_name": "birthday",
      |       "new_col_name": "d_o_b",
      |       "new_data_type": "date",
      |       "date_expression": "dd-MM-yyyy"
      |     }
      |   ]
      |}
    """.stripMargin


  "Application" should {

    "process configuration correctly" in {
      ArgParser.config(testArgs)
    }

    "process without errors" in {
      Application.main(Array(testArgs))
    }
  }

  private def getFileWithUtil(fileName: String): String = Try {
    getClass.getClassLoader.getResource(fileName).toString
  }.get
}
