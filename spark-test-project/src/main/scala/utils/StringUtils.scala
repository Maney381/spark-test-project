package utils

object StringUtils {
  def longestCommonSubstring(str1: String, str2: String): String = {
    val lengths = Array.ofDim[Int](str1.length + 1, str2.length + 1)
    var maxLength = 0
    var endIndex = 0

    for (i <- 1 to str1.length) {
      for (j <- 1 to str2.length) {
        if (str1(i - 1) == str2(j - 1)) {
          lengths(i)(j) = lengths(i - 1)(j - 1) + 1
          if (lengths(i)(j) > maxLength) {
            maxLength = lengths(i)(j)
            endIndex = i
          }
        }
      }
    }

    str1.substring(endIndex - maxLength, endIndex)
  }
}
