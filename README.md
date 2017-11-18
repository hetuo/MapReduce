How many records are in the dataset?<br>
Total lines 324810912

Are there any Geohashes that have snow depths greater than zero for the entire year? List some of the top Geohashes.<br>
snow	c41v48pupf00	94.841847<br>![](/image/image0.png)<br>

snow	c41ueb1jyypb	97.52479910000001<br>![](/image/image1.png)<br>
snow 	c1grzzxwt1bp 	96.27959772999996<br>![](/image/image2.png)<br>

When and where was the hottest temperature observed in the dataset? Is it an anomaly?<br>
Hottest 9g76dbr175ez 2015-04 329.7295<br>![](/image/image3.png)<br>
It is not an anomaly. I went to Mexico, it is extremely hot there. I dont want to go there anymore, never and ever!

Where are you most likely to be struck by lightning? Use a precision of 4 Geohash characters and provide the top 3 locations.<br>
lighting	cft9	9.990485252140819E-4<br>![](/image/image4.png)<br>
lighting	f4hx	9.990485252140819E-4<br>![](/image/image5.png)<br>
lighting 	f4we 	9.990485252140819E-4<br>![](/image/image6.png)<br>

What is the driest month in the bay area? This should include a histogram with data from each month. (Note: how did you determine what data points are in the bay area?)<br>
See driest.pdf<br>

I don't know how create the histogram automatically, so I used the python script to create this "histogram" with attribute moisture instead of temperature. From this picture, we can know that in bay area the average mositure level in march is the lowest during the whole year. So march is the driest month in the bay area.  <br>

  1 # Bay area driest<br>
  2 01  90.0    1.0 80802.54275810026   30.108865710560625<br><br>
  3 02  97.0    1.0 41408.41587239993   27.227335164835164<br>
  4 03  100.0   1.0 75620.94398159963   23.56841388699796<br>
  5 04  100.0   1.0 48470.329071200315  33.24974012474012<br>
  6 05  98.0    1.0 92340.67424319984   28.81575682382134<br>
  7 06  100.0   1.0 97271.60821730031   24.47441778405081<br>
  8 07  99.0    1.0 139001.72815919906  34.77459016393443<br>
  9 08  83.0    1.0 106286.97061500023  23.17535853976532<br>
 10 09  95.0    1.0 76322.76857489932   30.895505097312327<br>
 11 10  99.0    1.0 113630.45992830156  31.772380636604776<br>
 12 11  100.0   1.0 75199.26409569931   37.25371687136393<br>
 13 12  100.0   1.0 64651.31594539975   46.1183608058608<br>
<br>![](/image/image-driest.png)<br>



After graduating from USF, you found a startup that aims to provide personalized travel itineraries using big data analysis. Given your own personal preferences, build a plan for a year of travel across 5 locations. Or, in other words: pick 5 regions. What is the best time of year to visit them based on the dataset?<br>
  1 Best    c6  07  292.6152845697303 ![](/image/image7.png)<br>
  2 Best    dn  05  292.122054233223 ![](/image/image8.png)<br>
  3 Best    9q  10  291.42525366977986![](/image/image9.png)<br>
  4 Best    dr  07  294.12930454421934![](/image/image10.png)<br>
  5 Best    9x  06  293.1151512850738![](/image/image11.png)<br>

Your travel startup is so successful that you move on to green energy; here, you want to help power companies plan out the locations of solar and wind farms across North America. Write a MapReduce job that locates the top 3 places for solar and wind farms, as well as a combination of both (solar + wind farm). You will report a total of 9 Geohashes as well as their relevant attributes (for example, cloud cover and wind speeds).<br>
Best wind	f9p9	2.420077881605366<br>![](/image/image12.png)<br>
Best wind	fc0m	2.451849771413264<br>![](/image/image13.png)<br>
Best wind	f9pd	2.427110122015659<br>![](/image/image14.png)<br>
Best solar	9mwj	54.5285729359199<br>![](/image/image15.png)<br>
Best solar	9mty	54.86044352329938<br>![](/image/image16.png)<br>
Best solar	9mtz	54.69210675874115<br>![](/image/image17.png)<br>
Best sum	9mqe	54.93183878580474<br>![](/image/image18.png)<br>
Best sum	9mty	54.95136147020246<br>![](/image/image19.png)<br>
Best sum	d7d8	54.93401144114256<br>![](/image/image120.png)<br>

Given a Geohash prefix, create a climate chart for the region. This includes high, low, and average temperatures, as well as monthly average rainfall<br>
See climate.pdf<br>
  1 # Bay area<br>
  2 01  294.53485   270.60333   80802.54275809837   284.14202069099565<br>
  3 02  297.76508   270.21588   41408.41587239984   286.8383830494436<br>
  4 03  306.60608   271.0848    75620.94398159995   288.5873759019719<br>
  5 04  309.51392   270.46393   48470.32907120059   287.4621737525995<br>
  6 05  307.76978   276.15454   92340.67424320128   288.9143955024837<br>
  7 06  318.2832    278.19702   97271.60821730037   293.33153587861295<br>
  8 07  317.71118   277.70215   139001.72815919883  295.1986037011418<br>
  9 08  318.97388   278.45557   106286.97061500065  295.43758859192195<br>
 10 09  317.88818   278.1482    76322.7685748983    295.2435494439341<br>
 11 10  311.6577    270.60333   113630.45992829802  292.648857453582<br>
 12 11  299.20288   268.73056   75199.26409569876   284.92863531027297<br>
 13 12  306.60608   269.92175   64651.315945399256  283.28145431318814<br>
 
<br>![](/image/image-climate.png)<br> 


