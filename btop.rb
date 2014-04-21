require 'sinatra'
require 'mongo'
require 'time'

QUARTER_CONST = 15

connection = Mongo::Connection.new
collection = connection['btop']

# Enabling sessions
configure do
  enable :sessions
end

# Find current quarter value
def find_quarter()
  current_time = Time.new()

  return (current_time.min / QUARTER_CONST) + 1
end

# Routing for banner campaigns
get '/campaigns/:id' do
  current_quarter = find_quarter()
  clicks_collection = collection["clicks_#{current_quarter}"]
  data = find_banners(clicks_collection, params[:id].to_i)

  randomIndex = obtain_random_index(data.count)

  obj = data[randomIndex]
  "<img src='/images/image_#{obj['_id']['banner_id']}.png' title='Image from quarter# #{current_quarter}'/>"
end

# Finds and displays banners based on different performance numbers
def find_banners(collection, campaign_id)
  count_with_conversion = get_filtered_data(collection, campaign_id, 'revenue', -1, { "revenue" => { "$gt" => 0 } }).count

  if count_with_conversion.between?(5, 9)
    return get_filtered_data(collection, campaign_id, 'revenue', count_with_conversion)
  elsif count_with_conversion.between?(0, 4)
    top_revenue = []

    if count_with_conversion > 0
      top_revenue = get_filtered_data(collection, campaign_id, 'revenue', count_with_conversion)
    end

    top_clicks = get_filtered_data(collection, campaign_id, 'clicks', 5 - count_with_conversion)

    return top_revenue + top_clicks
  end

  return get_filtered_data(collection, campaign_id, 'revenue')
end

# Obtains random, but sequentially unique index per session
def obtain_random_index(data_count)
  index_array = Array.new(data_count){|i|i}
  if session['key']
    index_array.delete_at(session['key'])
  end

  randIndex = index_array[rand(0..index_array.count-1)]
  session['key'] = randIndex

  return randIndex
end

# Shortcut for Mongo query
def get_filtered_data(collection, campaign_id, sort = 'clicks', limit = 10, matcher = {})
  match = { "campaign_id" => campaign_id }
  match = match.merge(matcher)

  query = [
      {"$match" => match},
      {
          "$group" => {
              :_id => {
                  "campaign_id" => "$campaign_id",
                  "banner_id"   => "$banner_id"
              },
              :clicks  => { "$sum" => 1 },
              :revenue => { "$sum" => "$revenue" }
          }
      },
      { "$sort" => { "#{sort}" =>  -1 } }
  ]

  if limit != -1
    query.push({ "$limit" => limit })
  end

  return collection.aggregate(query)
end