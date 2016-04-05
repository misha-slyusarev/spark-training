from __future__ import print_function
from pyspark import SparkContext

import sys

if __name__ == "__main__":
    sc = SparkContext(appName="TopTen")

    # Read all files from the folder and split obtained data by lines;
    # wholeTextFiles will read them as tuples (fileName, fileContent), 
    # so we take uv[1] to work with file content
    userVisits = sc.wholeTextFiles("uservisits") \
        .flatMap(lambda uv: uv[1].split('\n')).filter(lambda uv: uv != '')

    # Print out first line
    print('First visitor: ' + userVisits.first())

    # Find top ten countries of most often visitors;
    # Country code is in 5th position in the string
    topTen = userVisits.map(lambda uv: (uv.split(',')[5], 1)) \
        .reduceByKey(lambda a, b: a + b).sortBy(lambda (country, count): count, False).take(10)

    print(topTen)


    sc.stop()
