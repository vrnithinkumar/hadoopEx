# Hadoop Secondary Sort

The MapReduce framework sorts the records by key before they reach the reducers. For any particular key, however, the values are not sorted. The order in which the values appear is not even stable from one run to the next, because they come from different map tasks, which may finish at different times from run to run. Generally speaking, most MapReduce programs are written so as not to depend on the order in which the values appear to the reduce function. However, it is possible to impose an order on the values by sorting and grouping the keys in a particular way.

To summarize, there is a recipe here to get the effect of sorting by value:
• Make the key a composite of the natural key and the natural value.
• The sort comparator should order by the composite key (i.e., the natural key and natural value).
• The partitioner and grouping comparator for the composite key should consider only the natural key for partitioning and grouping.
