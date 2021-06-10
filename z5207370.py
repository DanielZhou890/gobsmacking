import pandas as pd
from flask import Flask, request, send_file
from flask_restx import Resource, Api, fields, reqparse
import sqlite3
import numpy as np
from datetime import datetime, timedelta
import json
import matplotlib.pyplot as plt

app = Flask(__name__)
api = Api(app,
          default="TV Shows",  # Default namespace
          title="TV Shows",  # Documentation Title
          description="API to store TV shows from the external tvmaze API")

# Schedule schema
schedule_model = api.model('Schedule', {
    'time': fields.String,
    'days': fields.List(fields.String)
})

# Rating schema
rating_model = api.model('Rating', {
    'average': fields.Float
})

# Country schema
country_model = api.model('Country', {
    'name': fields.String,
    'code': fields.String,
    'timezone': fields.String
})

# Network schema
network_model = api.model('Network', {
    'id': fields.Integer,
    'name': fields.String,
    'country': fields.Nested(country_model)
})

# TV Show schema
# Note: 'id' and 'tvmaze_id' will never change once the TV show has been imported
shows_model = api.model('Shows', {
    'name': fields.String,
    'type': fields.String,
    'language': fields.String,
    'genres': fields.List(fields.String),
    'status': fields.String,
    'runtime': fields.Integer,
    'premiered': fields.Date,
    'officialSite': fields.Url,
    'schedule': fields.Nested(schedule_model),
    'rating': fields.Nested(rating_model),
    'weight': fields.Integer,
    'network': fields.Nested(network_model),
    'summary': fields.String
})

# Query parameter for importing a TV show
parser = reqparse.RequestParser()
parser.add_argument('name')

@api.route('/tv-shows/import')
@api.param('name', 'The TV show name')
class ShowsImport(Resource):

    @api.response(201, 'TV Show Created Successfully')
    @api.response(400, 'Bad Request')
    @api.doc(description="Add a new TV show")
    def post(self):
        # Get TV show name from query parameter
        args = parser.parse_args()
        name = args.get('name')
        conn = sqlite3.connect("z5207370.db")
        # Construct query
        query = 'http://api.tvmaze.com/search/shows?q='
        for word in name.split():
            query += word
            query += '%20'
        # Remove last ascii space if no name is given in the request
        if (name != ''):
            query = query[0:-3]
        # Convert query result to dataframe
        df = pd.read_json(query)
        # Flatten 'shows' field
        df = df.join(df['show'].apply(pd.Series))
        # Get all rows that match search name
        name_reformatted = name.replace(' ', '_')
        name_reformatted = name_reformatted.replace('-', '_')
        name_reformatted = name_reformatted.lower()
        df_name_copy = df[['id', 'name']].copy()
        df_name_copy.columns = ['id', 'name_old']
        df['name'] = df['name'].apply(lambda x: x.replace(' ', '_'))
        df['name'] = df['name'].apply(lambda x: x.replace('-', '_'))
        df['name'] = df['name'].apply(lambda x: x.lower())
        df = df[df['name'] == name_reformatted]
        df = df.merge(df_name_copy, on='id')

        # If requested TV show name does not match any TV shows from the tvmaze API
        if (df.shape[0] == 0):
            return {"message": "Invalid TV show"}, 400

        # If the TV show matches any TV shows already stored
        shows = pd.read_sql_query('select * from TV_Shows', con=conn)
        shows['name'] = shows['name'].apply(lambda x: x.replace(' ', '_'))
        shows['name'] = shows['name'].apply(lambda x: x.replace('-', '_'))
        shows['name'] = shows['name'].apply(lambda x: x.lower())
        shows = shows[shows['name'] == name_reformatted]
        if (shows.shape[0] > 0):
            return {"message": "TV show already exists in database"}, 400
            
        # Get first matching row
        df = df[['id', 'name_old', 'type', 'language', 'genres', 'status', 'runtime',\
                 'premiered', 'officialSite', 'schedule', 'rating', 'weight',\
                 'network', 'summary']].iloc[0].to_frame().transpose()
        df.columns = ['tvmaze_id', 'name', 'type', 'language', 'genres', 'status', 
                      'runtime', 'premiered', 'officialSite', 'schedule', 'rating',\
                      'weight', 'network', 'summary']

        # Cast 'tvmaze_id' to int
        df['tvmaze_id'] = int(df['tvmaze_id'].iloc[0])
        # Cast 'runtime' to int
        if not pd.isna(df['runtime'].iloc[0]):
            df['runtime'] = int(df['runtime'].iloc[0])
        # Cast 'weight' to int
        if not pd.isna(df['weight'].iloc[0]):
            df['weight'] = int(df['weight'].iloc[0])

        # Convert json object for genres to json string
        df['genres'] = df['genres'].apply(lambda x: json.dumps(x))
        # Convert json object for schedule to json string
        df['schedule'] = df['schedule'].apply(lambda x: json.dumps(x))
        # Convert json object for rating to just the average value
        df['rating'] = df['rating'].apply(lambda x: x['average'])
        # Convert json object for network to json string
        df['network'] = df['network'].apply(lambda x: json.dumps(x))

        # Create unique id for tv show
        id = pd.read_sql_query('select max(id) from TV_Shows', con=conn)
        id = id['max(id)'].iloc[0]
        if (id is None):
            id = 0
        else:
            id += 1
        # Add id to dataframe
        df['id'] = id

        # Get current date and time and format
        now = datetime.now()
        now = now.strftime('%Y-%m-%d %H:%M:%S')
        # Add current date and time to dataframe
        df['last_update'] = now

        # Reorder columns
        df = df[['tvmaze_id', 'id', 'last_update', 'name', 'type', 'language', 'genres', 
                 'status', 'runtime', 'premiered', 'officialSite', 'schedule', 'rating', 
                 'weight', 'network', 'summary']]
        pd.io.sql.to_sql(df, name="TV_Shows", con=conn, if_exists='append', index=False)
        conn.close()

        # Generate response body
        href = 'http://127.0.0.1:5000/tv-shows/import?name='
        for word in name.split():
            href += word
            href += '%20'
        if (name != ''):
            href = href[0:-3]
        response = {
            'id': df['id'].iloc[0].item(),
            'last_update': df['last_update'].iloc[0],
            'tvmaze_id': df['tvmaze_id'].iloc[0].item(),
            '_links': {
                'self': {
                    'href': href
                }
            }
        }

        return response, 201

@api.route('/tv-shows/<int:id>')
class Shows(Resource):

    @api.response(404, 'TV show was not found')
    @api.response(200, 'Successful')
    @api.doc(description="Get a TV show by its ID")
    def get(self, id):
        # Get all rows (only one row since id is unique) matching the supplied id
        conn = sqlite3.connect('z5207370.db')
        show = pd.read_sql_query('select * from TV_Shows where id=' + str(id), con=conn)
        # If no TV show in the database matches the requested id
        if (show.shape[0] == 0):
            return "TV show of id '{}' doesn't exist".format(id), 404
        
        # Bypass errors for non JSON serializable data types
        runtime = show['runtime'].iloc[0]
        rating = show['rating'].iloc[0]
        weight = show['weight'].iloc[0]
        if runtime is not None:
            runtime = runtime.item()
        if rating is not None:
            rating = rating.item()
        if weight is not None:
            weight = weight.item()

        # Generate response
        response = {
            'tvmaze_id': show['tvmaze_id'].iloc[0].item(),
            'id': show['id'].iloc[0].item(),
            'last_update': show['last_update'].iloc[0],
            'name': show['name'].iloc[0],
            'type': show['type'].iloc[0],
            'language': show['language'].iloc[0],
            'genres': json.loads(show['genres'].iloc[0]),
            'status': show['status'].iloc[0],
            'runtime': runtime,
            'premiered': show['premiered'].iloc[0],
            'officialSite': show['officialSite'].iloc[0],
            'schedule': json.loads(show['schedule'].iloc[0]),
            'rating': {
                'average': rating
            },
            'weight': weight,
            'network': json.loads(show['network'].iloc[0]),
            'summary': show['summary'].iloc[0]
        }

        # Generate _links field
        _links = {
            'self': {
                'href': 'http://127.0.0.1:5000/tv-shows/' + str(id)
            }
        }
        # Get the previous link if one exists
        prev = pd.read_sql_query('select * from TV_Shows where id<' + str(id) + ' order by id desc', con=conn)
        if (prev.shape[0] > 0):
            _links['previous'] = {
                'href': 'http://127.0.0.1:5000/tv-shows/' + str(prev['id'].iloc[0])
            }
        next = pd.read_sql_query('select * from TV_Shows where id>' + str(id) + ' order by id asc', con=conn)
        if (next.shape[0] > 0):
            _links['next'] = {
                'href': 'http://127.0.0.1:5000/tv-shows/' + str(next['id'].iloc[0])
            }
        
        # Add _links to the response
        response['_links'] = _links
        
        conn.close()

        return response, 200

    @api.response(404, 'TV show was not found')
    @api.response(200, 'Successful')
    @api.doc(description="Delete a TV show by its ID")
    def delete(self, id):
        # Check if the TV show exists
        conn = sqlite3.connect('z5207370.db')
        show = pd.read_sql_query('select * from TV_Shows where id=' + str(id), con=conn)
        # If no TV show in the database matches the requested id
        if (show.shape[0] == 0):
            return "TV show of id '{}' doesn't exist".format(id), 404

        # Delete TV show from database matching id
        c = conn.cursor()
        c.execute('delete from TV_Shows\
                       where id=' + str(id))
        conn.commit()
        conn.close()

        # Generate response
        response = {
            'message': 'The tv show with id ' + str(id) + ' was removed from the database!',
            'id': id
        }

        return response, 200

    @api.response(404, 'TV show was not found')
    @api.response(400, 'Bad request: invalid or incorrect field(s)')
    @api.response(200, 'Successful')
    @api.doc(description="Update a TV show by its ID")
    @api.expect(shows_model, validate=True)
    def patch(self, id):
        # Check if the TV show exists
        conn = sqlite3.connect('z5207370.db')
        show = pd.read_sql_query('select * from TV_Shows where id=' + str(id), con=conn)
        # If no TV show in the database matches the requested id
        if (show.shape[0] == 0):
            return "TV show of id '{}' doesn't exist".format(id), 404

        # Get request payload
        show = request.json

        # Check for extra payload fields
        for field in show:
            if field not in shows_model:
                return "Field '{}' is invalid".format(field), 400

        # Get current date and time and format
        now = datetime.now()
        now = now.strftime('%Y-%m-%d %H:%M:%S')

        # Construct sql query to update database
        c = conn.cursor()
        query = 'update TV_Shows set '
        for field in show:
            # Format the data
            data = "'"
            if (field == 'rating'):
                data += str(show['rating']['average'])
            elif (field == 'weight'):
                data += str(show[field])
            elif (field == 'schedule' or field == 'rating' or field == 'network' or field == 'genres'):
                data += json.dumps(show[field])
            else:
                data += str(show[field])
            query += field + ' = ' + data + "', "
        # Add last_update field to query
        query += "last_update = '" + now + "' where id = " + str(id)

        # Execute query
        c.execute(query)
        conn.commit()
        conn.close()

        # Generate response
        response = {
            'id': id,
            'last_update': now,
            '_links': {
                'self': {
                    'href': 'http://127.0.0.1:5000/tv-shows/' + str(id)
                }
            }
        }

        return response, 200

# Query arguments for retrieving a list of available TV shows
parser.add_argument('order_by')
parser.add_argument('page')
parser.add_argument('page_size')
parser.add_argument('filter')

@api.route('/tv-shows')
@api.param('order_by', 'The way the TV shows are ordered')
@api.param('page', 'The page number')
@api.param('page_size', 'The page size')
@api.param('filter', 'The fields to be displayed')
class ShowsDisplay(Resource):

    @api.response(200, 'TV Shows Successfully retrieved')
    @api.response(400, 'Bad Request')
    @api.response(404, 'TV Shows not found')
    @api.doc(description="Retrieve all available TV show based off of parameter(s)")
    def get(self):
        # Define default parameters
        params = {
            'order_by': "+id",
            'page': 1,
            'page_size': 100,
            'filter': "id,name",
        }

        # Get parameters from query
        args = parser.parse_args()
        args_check = False
        if args.get('order_by') is not None:
            params['order_by'] = args.get('order_by')
            args_check = True
        if args.get('page') is not None:
            params['page'] = int(args.get('page'))
            args_check = True
        if args.get('page_size') is not None:
            params['page_size'] = int(args.get('page_size'))
            args_check = True
        if args.get('filter') is not None:
            params['filter'] = args.get('filter')
            args_check = True

        # Reformat 'order_by' parameter
        params['order_by'] = params['order_by'].split(',')
        order_reformatted = {}
        for entry in params['order_by']:
            field = entry[1:]
            if (entry[0] == '+'):
                order_reformatted[field] = 'asc'
            elif (entry[0] == '-'):
                order_reformatted[field] = 'desc'
            else:
                return "Parameter 'order_by' is invalid: first part must either be '+' or '-'", 400
        
        # Check 'order_by' fields are valid
        for field in order_reformatted:
            if field not in ['id', 'name', 'runtime', 'premiered', 'rating']:
                return "Order_by field '{}' is invalid".format(field), 400

        # Reformat 'filter' parameter
        params['filter'] = params['filter'].split(',')

        # Check filters are valid
        for field in params['filter']:
            if field not in ['tvmaze_id', 'id', 'last_update', 'name', 'type', 'language', 'genres', 
                             'status', 'runtime', 'premiered', 'officialSite', 'schedule', 'rating', 
                             'weight', 'network', 'summary']:
                return "Filter '{}' is invalid".format(field), 400

        # Construct query
        query = 'select '
        for field in params['filter']:
            query += field + ', '
        query = query[0:-2]
        query += ' from TV_Shows order by '
        for field in order_reformatted:
            query += field + ' ' + order_reformatted[field] + ', '
        query = query[0:-2]
        offset = (params['page'] - 1) * params['page_size']
        query += ' limit ' + str(params['page_size']) + ' offset ' + str(offset)
        
        # Execute query
        conn = sqlite3.connect('z5207370.db')
        result = pd.read_sql_query(query, con=conn)

        # If no results are returned
        if (result.shape[0] == 0):
            return "No TV shows were found matching your search parameters", 404
        
        # Construct 'tv_shows' response field
        tv_shows = []
        for index, row in result.iterrows():
            show = {}
            for field in result.columns:
                show[field] = row[field]
            tv_shows.append(show)

        # Construct '_links['self']' response field
        self_url = 'http://127.0.0.1:5000/tv-shows'
        if args_check:
            self_url += '?'
            for param in ['order_by', 'page', 'page_size', 'filter']:
                if args.get(param) is not None:
                    self_url += param + '=' + str(args.get(param)) + '&'
            self_url = self_url[0:-1]
        links = {
            'self': {
                'href': self_url
            }
        }
        # Construct '_links['previous']' response field if it exists
        if (params['page'] > 1):
            prev_url = 'http://127.0.0.1:5000/tv-shows?'
            for param in ['order_by', 'page', 'page_size', 'filter']:
                if args.get(param) is not None:
                    if (param == 'page'):
                        prev_url += 'page' + '=' + str(params['page'] - 1) + '&'
                    else:
                        prev_url += param + '=' + str(args.get(param)) + '&'
            prev_url = prev_url[0:-1]
            links['previous'] = {
                'href': prev_url
            }
        # Construct '_links['next']' response field if it exists
        size = pd.read_sql_query('select count(id) as count\
                                      from TV_Shows', con=conn)
        if (params['page'] * params['page_size'] < int(size['count'].iloc[0])):
            next_url = 'http://127.0.0.1:5000/tv-shows?'
            for param in ['order_by', 'page', 'page_size', 'filter']:
                if args.get(param) is not None or (param == 'page'):
                    if (param == 'page'):
                        next_url += 'page' + '=' + str(params['page'] + 1) + '&'
                    else:
                        next_url += param + '=' + str(args.get(param)) + '&'
            next_url = next_url[0:-1]
            links['next'] = {
                'href': next_url
            }

        # Construct response
        response = {
            'page': params['page'],
            'page_size': params['page_size'],
            'tv_shows': tv_shows,
            '_links': links
        }

        return response, 200

# Query arguments for retrieving a list of available TV shows
parser.add_argument('format')
parser.add_argument('by')

@api.route('/tv-shows/statistics')
@api.param('format', 'The format the statistics should be presented')
@api.param('by', 'Statistics breakdown by this attribute')
class ShowsStatistics(Resource):

    @api.response(200, 'TV Shows Statistics Successfully retrieved')
    @api.response(400, 'Bad Request')
    @api.response(404, 'TV Shows not found')
    @api.doc(description="Retrieve statistics of all TV shows based off of a parameter")
    def get(self):
        # Get parameters from query
        args = parser.parse_args()

        params = {}
        # Check for invalid or missing parameters
        format_arg = args.get('format')
        if not (format_arg == 'json' or format_arg == 'image'):
            return "Format parameter '{}' is invalid".format(format_arg), 400
        else:
            params['format'] = format_arg
        by = args.get('by')
        if not by in ['language', 'genres', 'status', 'type']:
            return "By parameter '{}' is invalid".format(by), 400
        else:
            params['by'] = by

        # Get the total number of TV shows in the database
        conn = sqlite3.connect('z5207370.db')
        total = pd.read_sql_query('select count(id) as count\
                                       from TV_Shows', con=conn)
        total = int(total['count'].iloc[0])
        
        # Get the total number of TV shows in the database that have been updated in the last 24 hours
        yesterday = datetime.now() - timedelta(days=1)
        yesterday = yesterday.strftime('%Y-%m-%d %H:%M:%S')
        yesterday = pd.read_sql_query("select count(id) as count\
                                           from TV_Shows\
                                           where last_update > '" + yesterday + "'", con=conn)
        updated = int(yesterday['count'].iloc[0])

        # Get the breakdown of the statistics for all the TV shows in the database
        if (params['by'] != 'genres'):
            stats = pd.read_sql_query('select ' + params['by'] + ', count(*) as percent\
                                           from TV_Shows\
                                           group by ' + params['by'], con=conn)
            # Error check for empty database
            if (stats.shape[0] == 0):
                return "No TV shows have been imported into the database", 404
            # Add row stating missing data entries if it exists
            stats_sum = stats['percent'].sum()
            if (stats_sum < total):
                df = {
                    stats_sum.columns[0]: 'Missing data',
                    'percent': total - stats_sum['percent']
                }
                df = pd.DataFrame(data=df)
                stats_sum = stats_sum.append(df)

            stats['percent'] = stats['percent'].apply(lambda x: x * 100 / total )
        else:
            stats = pd.read_sql_query('select genres\
                                            from TV_Shows', con=conn)
            # Error check for empty database
            if (stats.shape[0] == 0):
                return "No TV shows have been imported into the database", 404
            stats = stats[stats['genres'] != '[]']
            # Explode rows in list to multiple rows
            stats['genres'] = stats['genres'].apply(lambda x: x[1:-1])
            stats = pd.concat([pd.Series(row['genres'].split(', ')) for index, row in stats.iterrows()]).reset_index()
            stats.columns = ['index', 'genres']
            stats = stats[['genres']]
            # Remove quotation marks for strings
            stats['genres'] = stats['genres'].apply(lambda x: x[1:-1])
            # Get the percentage of each genre relative to the entire database
            stats = stats['genres'].value_counts()
            stats = stats.to_frame()
            stats = stats.reset_index()
            stats.columns = ['genres', 'percent']
            stats['percent'] = stats['percent'].apply(lambda x: x * 100 / total)
        
        # Round percent to 2 decimal places
        stats['percent'] = stats['percent'].apply(lambda x: round(x, 2))
        
        # Construct response for JSON
        if (params['format'] == 'json'):
            values = {}
            for index, row in stats.iterrows():
                values[row[row.index[0]]] = row['percent']
            response = {
                'total': total,
                'total_updated': updated,
                'values': values
            }
            return response, 200
        # Construct response for image
        else:
            # Generate labels and title
            labels = stats[stats.columns[0]].tolist()
            for i in range(len(labels)):
                labels[i] += ' (' + str(stats['percent'].iloc[i]) + '%)'
            title = 'Percentage distribution of {} of all movies in the database'.format(params['by'])
            # Pie charts for language, status and type
            if (params['by'] != 'genres'):
                ax = stats.plot.pie(y='percent', figsize=(15, 10), labels=labels, title=title)
                ax.set_ylabel('')
                ax.annotate('Total number of TV shows in database: ' + str(total), (1, -1), weight='bold')
                ax.annotate('Total number of TV updated in the past 24 hours: ' + str(updated), (1, -1.05), weight='bold')
            # Bar chart for genres
            else:
                ax = stats.plot(kind='bar', x='genres', y='percent', figsize=(15, 10), rot=0)
                ax.set_xlabel('Genres', weight='bold')
                ax.set_ylabel('Percentage', weight='bold')
                plt.title(title, weight='bold')
                plt.legend(labels=[])
                plt.subplots_adjust(right=0.8)
                ax.annotate('Total number of TV \nshows in database: ' + str(total), (1210, 150), weight='bold', xycoords='figure pixels')
                ax.annotate('Total number of TV \nupdated in the past 24 hours: ' + str(updated), (1210, 100), weight='bold', xycoords='figure pixels')

            plt.savefig('q6.png')
            
            return send_file('q6.png', cache_timeout=0)

if __name__ == '__main__':
    conn = sqlite3.connect('z5207370.db')
    c = conn.cursor()
    c.execute('create table if not exists TV_Shows (\
                   tvmaze_id integer not null check (tvmaze_id >= 0),\
                   id integer not null unique check (id >= 0),\
                   last_update datetime not null,\
                   name varchar(255) not null,\
                   type varchar(255),\
                   language varchar(255),\
                   genres varchar(255),\
                   status varchar(255),\
                   runtime integer,\
                   premiered varchar(255),\
                   officialSite varchar(255),\
                   schedule varchar(255),\
                   rating integer,\
                   weight integer,\
                   network varchar(1000),\
                   summary varchar(1000))')
    conn.commit()
    conn.close()
    app.run(debug=True)