require 'sinatra'
require 'mongo'
require 'time'

QUARTER_CONST = 15

connection = Mongo::Connection.new
collection = connection['btop']['clicks']

# Routing for banner campaigns
get '/campaigns/:id' do
  data = find_banners(collection, params[:id].to_i)

  randIndex = rand(0..data.count-1)
  obj = data[randIndex]
  "<img src='/images/image_#{obj['_id']['banner_id']}.png'/>"
end

# Find current quarter value
def find_quarter
  current_time = Time.new()
  return current_time.min / QUARTER_CONST
end

# Finds and displays banners based on different performance numbers
def find_banners(collection, campaign_id)
  count_with_conversion = 15

  if count_with_conversion.between?(5, 9)
    return get_filtered_data(collection, campaign_id, 'revenue', count_with_conversion)
  elsif count_with_conversion.between?(0, 4)
    top_revenue = get_filtered_data(collection, campaign_id, 'revenue', count_with_conversion)
    top_clicks = get_filtered_data(collection, campaign_id, 'clicks', 5 - count_with_conversion)

    return top_revenue + top_clicks
  end

  return get_filtered_data(collection, campaign_id, 'revenue')
end

# Shortcut for Mongo query
def get_filtered_data(collection, campaign_id, sort = 'clicks', limit = 10, matcher = {})
  match = { "campaign_id" => campaign_id }
  match = match.merge(matcher)

  return collection.aggregate(
    [
      {"$match" => match},
      {
         "$group" => {
           :_id => {
             "campaign_id" => "$campaign_id",
             "banner_id"   => "$banner_id"
           },
           :clicks => { "$sum" => 1 }
         }
      },
      { "$sort" => { "#{sort}" =>  -1 } },
      { "$limit" => limit }
    ]
  )
end