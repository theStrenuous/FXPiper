import boto
import os
import yaml

# f = open('FX.html','w')

timestamp = "2017-04-30"

FX_html = """
<html>
    <head>
        <title>Forex Trading Schedule</title>
    </head>
    <body>
        <h1 align="center">Forex Trading Schedule</h1>
        <p><center><h2>USD/JPY</h2></center>
            <img src="USDJPY1.jpg" style="float: left; width: 45%; margin-left: 2%;margin-right: 0%; margin-bottom: 3%;">
            <img src="USDJPY2.jpg" style="float: right; width: 38%; height: 62%; margin-right: 10%; margin-bottom: 3%;">
        </p>
        <p style="clear: both;">
        <p><center><h2>GBP/JPY</h2></center>
            <img src="GBPJPY1.jpg" style="float: left; width: 45%; margin-left: 2%;margin-right: 0%; margin-bottom: 3%;">
            <img src="GBPJPY2.jpg" style="float: right; width: 38%; height: 62%; margin-right: 10%; margin-bottom: 3%;">
        </p>
        <p style="clear: both;">
        <p><center><h2>EUR/USD</h2></center>
            <img src="EURUSD1.jpg" style="float: left; width: 45%; margin-left: 2%;margin-right: 0%; margin-bottom: 3%;">
            <img src="EURUSD2.jpg" style="float: right; width: 38%; height: 62%; margin-right: 10%; margin-bottom: 3%;">
        </p>
        <p style="clear: both;">
        <p><center><h2>EUR/GBP</h2></center>
            <img src="EURGBP1.jpg" style="float: left; width: 45%; margin-left: 2%;margin-right: 0%; margin-bottom: 3%;">
            <img src="EURGBP2.jpg" style="float: right; width: 38%; height: 62%; margin-right: 10%; margin-bottom: 3%;">
        </p>
        <p style="clear: both;">
    </body>
    <footer>
        <p><font size="4"><center>Streaming Data Used in Graph: till <strong>{timestamp}</strong><center></font></p>
    </footer>
</html>""".format(timestamp=timestamp)

# f.write(FX_html)
# f.close()

credentials = yaml.load(open(os.path.expanduser('api_cred.yml')))
conn = boto.connect_s3(credentials['AWS'].get('access_key_ID'),
                       credentials['AWS'].get('secret_access_key'))
bucket = conn.get_bucket("strenuousfx")
output_file = bucket.new_key('FX.html')
output_file.content_type = 'html'
output_file.set_contents_from_string(FX_html, policy='public-read')
