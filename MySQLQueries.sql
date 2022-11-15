#Query to count the number of flights cancelled along with the cancellation code
# A = carrier, B = weather, C = NAS, D = security, N=None
select sys.dataset.CancellationCode, count(sys.dataset.CancellationCode)
from sys.dataset
group by sys.dataset.CancellationCode;

#Query to calculate number of flights diverted
select sys.dataset.Diverted, count(sys.dataset.Diverted)
from sys.dataset
group by sys.dataset.Diverted;

#Adding a label column to store the results - whether slightly delayed, highly delayed or other
Alter table sys.dataset add label int;

# If slightly delayed = 0, highly delayed =1, other =2
UPDATE  sys.dataset
SET     sys.dataset.label = IF((sys.dataset.CancellationCode='N' and sys.dataset.Diverted=0 and sys.dataset.ArrDelay<=15),0, IF((sys.dataset.CancellationCode='N' and sys.dataset.Diverted=0 and sys.dataset.ArrDelay>15),1,2));

# Checking the distribution of the label values
select sys.dataset.label, count(sys.dataset.label)
from sys.dataset
group by sys.dataset.label;

# Sub splitting the value 2 (other) into 2 values - Diverted=3, Cancelled=2
UPDATE  sys.dataset
SET     sys.dataset.label = IF((sys.dataset.CancellationCode!='N'),2, IF((sys.dataset.Diverted=1),3,sys.dataset.label));

#Checking the distribution of delayedness, diverted, and cancelled 
select sys.dataset.label, count(sys.dataset.label)
from sys.dataset
group by sys.dataset.label;
