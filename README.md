# SCCA club points
This is a little python program I wrote to calculate class points for our local regions here in socal. 
I was wanting to figure out where I stood in points without having to manually calculate everything. 
Plus I wanted to try some web scraping. 


## Requirements
This code should run on any system that can run the python interpreter, and has been used on OSX and Linux using python 3.7+.

## Installation
Assuming a working python 3.x environment. 
Execute `pip install -r requirements.txt`  this will install any required modules for the script.

## Usage
1. Configure the script by editing the config.ini file.  Select San Diego region (sdr) or Calclub region (calclub)
2. Initialize the database by running the script without any switches.  
   1. Start building the data by webscraping.  This is done by using the -u switch and the url to the results. 
      `./clubpoints -u https://sdrscca.com/solo2/results/2022/event-02%202022-02-27-Final_Web.htm`
      It is preferred to input events in chronological order.  It is easiest to save the urls to a file, one per line and then loop over the file.
      In bash this could be done something like this. `for u in $(cat sdr_urls); do ./clubpoints.py -u $u; done`
   2. Two files are handy one with driver (pax) points urls and the other with class URLS. 
3. Once the results are scraped and inserted into the database, printing results are as simple as running the script with the -p switch or --driver for pax standings.
4. National Events.
   1. Both regions grant participants an average point score if they are attending a national level event that conflicts with a local event. 
      To update the record, it is as simple as inputting the car number, class, and event date.  `./clubpoints.py -n 97 -c CS -d 04-03-2022` will create an entry in both the drivers (PAX) results and class results and calculate the average points for that driver based on attended local event results for that class.<br>



## Switches
`-u/--url`  Url to webscrape<br>
`-g/--generate` Used to update the results table with the class data and drivers data.<br>
`-p/--print` Prints class results as text.<br>
`-n/--national` Accepts a car number along with event date and class, this is for people who have attened a national event and are awarded the average score of their local events for that missed event.<br>
`-d/--date` Date input in the format of MM-DD-YYYY.<br>
`-c/--class` Car Class ie.  CS, PAX, M1.<br>
`--driver` Output the driver pax standings.<br>
`-f/--filename` Filename for CSV output file, defaults to "results.csv"


## To Do
Utilize file output to csv, possibly add pdf output.  
streamline code, refactor bits of it.  

