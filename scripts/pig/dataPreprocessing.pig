register 'optimizationML-1.0-SNAPSHOT.jar';

DEFINE fe com.yahoo.ycp.optimizationML.udf.featureExtractor();

data = load '/user/dapr_dev/optimization1/common/groupImp/DONE/201509{08,09,10,11,12,13,14,15,16,17,18,19,20,21,22}00/*' using PigStorage(',');
-- data = foreach data generate $0 as name, $1 as subcat, $2 as cat, ($3 * 1+$4 * 2+$5 * 3+$6 * 4+$7 * 5+$8 * 6+$9 * 7+$10 * 8+$11 * 9+$12 * 10+$13 * 11+$14 * 12+$15 * 13) as age, ($16*1+$17*2+$18*3) as gender, ($19*1+$20*2+$21*3+$22*4+$23*5+$24*6+$25*7+$26*8+$27*9+$28*10+$29*11+$30*12+$31*13+$32*14+$33*15+$34*16+$35*17+$36*18+$37*19+$38*20+$39*21+$40*22+$41*23+$42*24+$43*25+$44*26+$45*27+$46*28+$47*29+$48*30+$49*31+$50*32+$51*33+$52*34+$53*35+$54*36+$55*37+$56*38+$57*39+$58*40+$59*41+$60*42+$61*43+$62*44+$63*45+$64*46+$65*47+$66*48+$67*49+$68*50+$69*51) as geo, $71 as clk;
data = foreach data generate fe($0..);

nameGrp = group data by name;
nameGrpClk = foreach nameGrp generate group as name, SUM(data.clk) as clkNum;
nameGrpClkExist = filter nameGrpClk by clkNum > 0;
data = join data by name, nameGrpClkExist by name;

pSubcatList = foreach data generate subcat;
pSubcatList = DISTINCT pSubcatList;
store pSubcatList into '/user/gdongrid/xinghai/pSubcatList' using PigStorage(',');

-- compute CTR for each age, gender, geo group
ageGrp = group data by age;
ageCtr = foreach ageGrp generate group as age, COUNT(data) as imp, SUM(data.clk) as clk, ( (COUNT(data) > 0) ? ((float) SUM(data.clk) / (float) COUNT(data) ) : 0 ) as ctr;
store ageCtr into '/user/gdongrid/xinghai/ycp/ageCtr1' using PigStorage(',', '-schema');

genderGrp = group data by gender;
genderCtr = foreach genderGrp generate group as gender, COUNT(data) as imp, SUM(data.clk) as clk, ( (COUNT(data) > 0) ? ((float) SUM(data.clk) / (float) COUNT(data) ) : 0 ) as ctr;
store genderCtr into '/user/gdongrid/xinghai/ycp/genderCtr1' using PigStorage(',', '-schema');

ageGenGrp = group data by age, gender;
ageGenCtr = foreach ageGenGrp generate group.age as age, group.gender as gender, COUNT(data) as imp, SUM(data.clk) as clk, ( (COUNT(data) > 0) ? ((float) SUM(data.clk) / (float) COUNT(data) ) : 0 ) as ctr;
store ageGenCtr into '/user/gdongrid/xinghai/ycp/ageGenCtr1' using PigStorage(',', '-schema');

geoGrp = group data by geo;
geoCtr = foreach geoGrp generate group as geo, COUNT(data) as imp, SUM(data.clk) as clk, ( (COUNT(data) > 0) ? ((float) SUM(data.clk) / (float) COUNT(data) ) : 0 ) as ctr;
store geoCtr into '/user/gdongrid/xinghai/ycp/geoCtr1' using PigStorage(',', '-schema');

-- generate training set
dataWithCtr1 = join data by age, ageCtr by age;
dataWithCtr11 = foreach dataWithCtr1 generate data::name as name, data::subcat as subcat, data::cat as cat, ageCtr::ctr as ageCtr, data::gender as gender, data::geo as geo, data::clk as clk, data::age as age;

dataWithCtr2 = join dataWithCtr11 by gender, genderCtr by gender;
dataWithCtr21 = foreach dataWithCtr2 generate dataWithCtr11::name as name, dataWithCtr11::subcat as subcat, dataWithCtr11::cat as cat, dataWithCtr11::ageCtr as ageCtr, genderCtr::ctr as genderCtr, dataWithCtr11::geo as geo, dataWithCtr11::clk as clk, dataWithCtr11::age as age, dataWithCtr11::gender as gender;

dataWithCtr3 = join dataWithCtr21 by geo, geoCtr by geo;
dataWithCtr31 = foreach dataWithCtr3 generate dataWithCtr21::name as name, dataWithCtr21::subcat as subcat, dataWithCtr21::cat as cat, dataWithCtr21::ageCtr as ageCtr, dataWithCtr21::genderCtr as genderCtr, geoCtr::ctr as geoCtr, dataWithCtr21::clk as clk, dataWithCtr21::age as age, dataWithCtr21::gender as gender;

dataWithCtr4 = join dataWithCtr31 by (age, gender), aegGenCtr by (age, gender);
dataWithCtr = foreach dataWithCtr4 generate dataWithCtr31::name as name, dataWithCtr31::subcat as subcat, dataWithCtr31::cat as cat, dataWithCtr31::ageCtr as ageCtr, dataWithCtr31::genderCtr as genderCtr, dataWithCtr31::geoCtr as geoCtr, ageGenCtr::ctr as agegenctr, dataWithCtr31::clk as clk, 1 as joinFactor;

dataAll = group dataWithCtr ALL;
dataStats = foreach dataAll generate COUNT(dataWithCtr) as impNum, SUM(dataWithCtr.clk) as clkNum, 1 as joinFactor;
dataWithCtrAndFactor = join dataWithCtr by joinFactor, dataStats by joinFactor;
trainingData = filter dataWithCtrAndFactor by (dataWithCtr::clk == 1 OR RANDOM() < ((dataStats::impNum == 0)? 0 : (float)((float) dataStats::clkNum * (float) 2.5 / (float) dataStats::impNum) ) );
trainingData = foreach trainingData generate dataWithCtr::name as name, dataWithCtr::subcat as subcat, dataWithCtr::cat as cat, dataWithCtr::ageCtr as ageCtr, dataWithCtr::genderCtr as genderCtr, dataWithCtr::geoCtr as geoCtr, dataWithCtr::clk as clk;

store trainingData into '/user/gdongrid/xinghai/ycp/sampledTrainingData1' using PigStorage(',', '-schema') ;

-- process testing data
testData = load '/user/dapr_dev/optimization1/common/groupImp/DONE/201509{23,24,25,26,27,28,29}00/*' using PigStorage(',');
testData = foreach testData generate $0 as name, $1 as subcat, $2 as cat, ($3 * 1+$4 * 2+$5 * 3+$6 * 4+$7 * 5+$8 * 6+$9 * 7+$10 * 8+$11 * 9+$12 * 10+$13 * 11+$14 * 12+$15 * 13) as age, ($16*1+$17*2+$18*3) as gender, ($19*1+$20*2+$21*3+$22*4+$23*5+$24*6+$25*7+$26*8+$27*9+$28*10+$29*11+$30*12+$31*13+$32*14+$33*15+$34*16+$35*17+$36*18+$37*19+$38*20+$39*21+$40*22+$41*23+$42*24+$43*25+$44*26+$45*27+$46*28+$47*29+$48*30+$49*31+$50*32+$51*33+$52*34+$53*35+$54*36+$55*37+$56*38+$57*39+$58*40+$59*41+$60*42+$61*43+$62*44+$63*45+$64*46+$65*47+$66*48+$67*49+$68*50+$69*51) as geo, $71 as clk;

testDataWithCtr1 = join testData by age, ageCtr by age;
testDataWithCtr11 = foreach testDataWithCtr1 generate data::name as name, data::subcat as subcat, data::cat as cat, ageCtr::ctr as ageCtr, data::gender as gender, data::geo as geo, data::clk as clk, data::age as age;

testDataWithCtr2 = join testDataWithCtr11 by gender, genderCtr by gender;
testDataWithCtr21 = foreach testDataWithCtr2 generate testDataWithCtr11::name as name, testDataWithCtr11::subcat as subcat, testDataWithCtr11::cat as cat, testDataWithCtr11::ageCtr as ageCtr, genderCtr::ctr as genderCtr, testDataWithCtr11::geo as geo, testDataWithCtr11::clk as clk, testDataWithCtr11::age as age, testDataWithCtr11::gender as gender;

testDataWithCtr3 = join testDataWithCtr21 by geo, geoCtr by geo;
testDataWithCtr31 = foreach testDataWithCtr3 generate testDataWithCtr21::name as name, testDataWithCtr21::subcat as subcat, testDataWithCtr21::cat as cat, testDataWithCtr21::ageCtr as ageCtr, testDataWithCtr21::genderCtr as genderCtr, geoCtr::ctr as geoCtr, testDataWithCtr21::clk as clk, testDataWithCtr21::age as age, testDataWithCtr21::gender as gender;

testDataWithCtr4 = join testDataWithCtr31 by (age, gender), aegGenCtr by (age, gender);
testDataWithCtr = foreach testDataWithCtr4 generate testDataWithCtr31::name as name, testDataWithCtr31::subcat as subcat, testDataWithCtr31::cat as cat, testDataWithCtr31::ageCtr as ageCtr, testDataWithCtr31::genderCtr as genderCtr, testDataWithCtr31::geoCtr as geoCtr, ageGenCtr::ctr as agegenctr, testDataWithCtr31::clk as clk, 1 as joinFactor;

store testDataWithCtr into '/user/gdongrid/xinghai/ycp/testDataWithCtr1' using PigStorage(',', '-schema') ;
