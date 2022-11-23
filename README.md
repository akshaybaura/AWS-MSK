# Data Engineering Assessment Test:

IMPORTANT:
- Build the system on a cloud environment.


INSTRUCTIONS:

Download the data from https://www.kaggle.com/wordsforthewise/lending-club. Use the file called rejected_2007_to_2018Q4.csv for the following assignment:
1. Using just the data in 2007, we are interested in the Moving Average of the Risk_Score column (50 rows back). Calculate the MA(50) of the Risk_Score and put it in a new column called RiskScoreMA50. Store this in a Data Store of your choice.
2. Starting in year 2008 to 2009, mock up a Kafka Stream of the data coming into the system. Calculate the MA50 for the incoming Risk Score as well. Then store the data in the data store.
3. Starting year 2009 onwards, you get a new requirement to also calculate the MA100 of the Risk Score and store it, but the data schema must also to be trackable. Make a new column on the data store that has a new column called MA100, update the version of this data store, and do version controlling so that the original features can easily be accessed.
4. The team found out that they did not actually want the MA50, but the EMA50 for the purpose of this calculation (want to edit the feature without changing the name). In this case, they do not want a new column for the EMA50, but replace the existing MA50 with new logic. Edit the logic for the feature creation pipeline, and version control the new version of the feature store.


Bonus Points:

1. Submission of a video demo of the system you built.

