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