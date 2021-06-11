This project explores WA state's I-502 data (public marijuana sales).
The data set is 400GBs unzipped and can be accessed by either a public
records request or through this [link](https://lcb.app.box.com/s/9o9b2ryi42ooj5pmqdv63c7lq9l7daea?page=1)
if it is still active.

Many of the paths in this project are absolute due to storing the data on a separate SDD
while conducting this analysis. Those will need to be replaced with your path to replicate this project.
In addition the datapipe class is a pyspark wrapper which can have issues based on pyspark's configuration.
