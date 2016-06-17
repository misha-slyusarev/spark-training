from __future__ import print_function
from pyspark import SparkContext
import re

def print_stats(rdd):
	num_lines      = rdd.count()
	sample_lines   = rdd.sample(False, min(1.0, 5.0 / float(num_lines))).collect()
	num_partitions = rdd.getNumPartitions()
	storage_level  = rdd.getStorageLevel()

	print( 'RDD statistics:')
	print( 'RDD ID: %d' % rdd.id())
	print( 'RDD Number of partitions: %d' % num_partitions)
	print( 'RDD Storage level: %s' % storage_level)
	print( 'Number of lines: %d' % num_lines)
	print( 'Sample lines:')

	for line in sample_lines:
		print('    %s' % str(line))

	return


# Return post id and post tags
def parse_line(line):
	parsed = re.findall("( Id|Tags)=\"(.+?)\"\s+", line)
	postId = int(parsed[0][1])
	tagsLine = ''
	if len(parsed) == 2:
		tagsLine = parsed[1][1]

	return postId, tagsLine

# Return a list of pairs (ID, TAG)
def expand_tags(pair):
	id = pair[0]
	tags = filter(None, re.split("&[lg]t;", pair[1]))
	return [(id, tag) for tag in tags]

if __name__ == "__main__":
	sc = SparkContext(appName="TagRecommender")

    raw = sc.textFile('hdfs://big-data:8020/posts')

	# 1. Consider only '<row' lines (which is an indicator of a post)
	# 2. Parse posts and extract pairs of Id and Tags (ID, "<TAG1><TAG2>")
	# 3. Filter out posts with empty Tags field
	# 4. Expand tags into a list of (ID, TAG) pairs
	pairs = raw.filter(lambda l: l.lstrip().startswith('<row')) \
		.map(lambda line: parse_line(line)) \
		.filter(lambda pair: pair[1]) \
		.flatMap(lambda pair: expand_tags(pair))

	print('Collected tags with post ids:')
	for pair in pairs.take(10):
		print(pair)

	sc.stop()

# u'  <row Id="4" PostTypeId="1" AcceptedAnswerId="7" CreationDate="2008-07-31T21:42:52.667" Score="385" ViewCount="26265" Body="&lt;p&gt;I want to use a track-bar to change a form\'s opacity.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;This is my code:&lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;decimal trans = trackBar1.Value / 5000;&#xA;this.Opacity = trans;&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;&#xA;&lt;p&gt;When I try to build it, I get this error:&lt;/p&gt;&#xA;&#xA;&lt;blockquote&gt;&#xA;  &lt;p&gt;Cannot implicitly convert type \'decimal\' to \'double\'.&lt;/p&gt;&#xA;&lt;/blockquote&gt;&#xA;&#xA;&lt;p&gt;I tried making &lt;code&gt;trans&lt;/code&gt; a &lt;code&gt;double&lt;/code&gt;, but then the control doesn\'t work. This code has worked fine for me in VB.NET in the past. &lt;/p&gt;&#xA;" OwnerUserId="8" LastEditorUserId="5455605" LastEditorDisplayName="Rich B" LastEditDate="2015-12-23T21:34:28.557" LastActivityDate="2016-02-11T04:17:34.323" Title="When setting a form\'s opacity should I use a decimal or double?" Tags="&lt;c#&gt;&lt;winforms&gt;&lt;type-conversion&gt;&lt;decimal&gt;&lt;opacity&gt;" AnswerCount="13" CommentCount="1" FavoriteCount="30" CommunityOwnedDate="2012-10-31T16:42:47.213" />'
