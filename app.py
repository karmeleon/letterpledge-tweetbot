import psycopg2, os, datetime, signal, sys
from urllib.parse import urlparse
from TwitterAPI import TwitterAPI
from configparser import ConfigParser

pg_conn = None

def main():
	# read the dev config file if it exists
	config = ConfigParser()
	config.read('settings-dev.ini')
	if not config.sections():
		config.read('settings.ini')
		if not config.sections():
			print('No settings file found, create settings.ini in this directory')
			exit()
		else:
			# this is the prod file, get the environment variables
			# instead of reading it literally
			for key in config['TwitterAPI']:
				try:
					config['TwitterAPI'][key] = os.environ[config['TwitterAPI'][key]]
				except KeyError:
					print('Missing environment var {}, fix your config dammit'.format(config['TwitterAPI'][key]))
					exit(1)

	# get the Twitter API going
	twitter_section = config['TwitterAPI']
	api = TwitterAPI(
		twitter_section['ConsumerKey'],
		twitter_section['ConsumerSecret'],
		twitter_section['OAuthAccessToken'],
		twitter_section['OAuthAccessSecret'],
	)

	# connect to PostgreSQL
	# use a heroku environment variable if available
	try:
		db_string = os.environ['DATABASE_URL']
		url = urlparse(db_string)
		pg_con = psycopg2.connect(
			database=url.path[1:],
			user=url.username,
			password=url.password,
			host=url.hostname,
			port=url.port,
		)
	except KeyError:
		# otherwise, fall back to the config file
		postgresql_section = config['PostgreSQL']
		pg_con = psycopg2.connect('dbname={} user={}'.format(
			postgresql_section['Database'],
			postgresql_section['User'])
		)
	

	pg_cur = pg_con.cursor()

	# start reading tweets
	tweets = api.request('statuses/filter', {'follow': config['General']['TrumpTwitterID']})
	print('Now listening for tweets')
	for incoming_tweet in tweets:
		if incoming_tweet['user']['id'] == config['General']['TrumpTwitterID']:
			print('Got Trump tweet: {}'.format(incoming_tweet['text']))
			# check for dupes, even though it shouldn't happen
			pg_cur.execute('SELECT * FROM tweets WHERE (twitter_id::text = %s::text)', (incoming_tweet['id'],))
			possible_dupe = pg_cur.fetchone()
			if not possible_dupe:
				print('And it\'s unique!')
				pg_cur.execute('INSERT INTO tweets (text, length, date, created_at, updated_at, twitter_id) VALUES (%s, %s, %s, %s, %s, %s)', (
					incoming_tweet['text'],
					len(incoming_tweet['text']),
					datetime.datetime.now(),
					datetime.datetime.now(),
					datetime.datetime.now(),
					incoming_tweet['id'],
				))
				pg_con.commit()
				print('Saved')

def signal_handler(signal, frame):
	print('Got SIGINT, closing DB connection')
	pg_conn.close()
	sys.exit(0)

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler)
	main()