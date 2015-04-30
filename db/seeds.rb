# This file should contain all the record creation needed to seed the database with its default values.
# The data can then be loaded with the rake db:seed (or created alongside the db with db:setup).
#
# Examples:
#
#   cities = City.create([{ name: 'Chicago' }, { name: 'Copenhagen' }])
#   Mayor.create(name: 'Emanuel', city: cities.first)

#########
# Leagues
#########

# Array of valid leagues for the SportsScraper. This structure is an Array of Arrays, where the elements are:
# id, League Name, Sport Friendly name
leagues = [[1, "NBA", "Basketball"],
 [nil, "NASCAR", "Racing"],
 [3, "NFL", "Football"],
 [nil, "NCB", "Basketball"],
 [nil, "NCW", "Basketball"],
 [8, "WNBA", "Basketball"],
 [7, "NCF", "Football"],
 [9, "MLS", "Soccer"],
 [6, "PGA", "Golf"],
 [4, "MLB", "Baseball"],
 [5, "NHL", "Hockey"]]

leagues.each do |league|
  League.find_or_create_by('LeagueName' => league[0], 'LeagueName' => league[1], 'SportName' => league[2])
end

#######
# Tasks
#######

Task.find_each { |t| t.stop! }
League.find_each do |league|
  Task.find_or_create_by(league_name: league.LeagueName)
end