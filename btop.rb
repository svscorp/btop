require 'sinatra'
require 'mongo'
require 'time'

QUARTER_CONST = 15

connection = Mongo::Connection.new
collection = connection['btop']['clicks']

# Find current quarter value
def find_quarter
  current_time = Time.new()
  return current_time.min / QUARTER_CONST
end

# Routing for banner campaigns
get '/campaigns/:id' do
  data = collection.aggregate(
            [
            {"$match" => { "campaign_id" => params[:id].to_i }},
            {
                "$group" => {
                    :_id => {
                        "campaign_id" => "$campaign_id",
                        "banner_id"   => "$banner_id"
                    },
                    :clicks => { "$sum" => 1 }
                }
            }, { "$sort" => { "clicks" =>  -1 } }, { "$limit" => 5 }])


  obj = data[rand(1..5)]
  "<img src='/images/image_#{obj['_id']['banner_id']}.png'/>"
end