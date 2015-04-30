class AddColumnsToPlayerStats < ActiveRecord::Migration
  def change
    add_column :PlayerStats_Basketball, :Points, :tinyint
    add_column :PlayerStats_Basketball, :Minutes, :tinyint

    add_column :PlayerStats_Golf, :FairwayPercent, :decimal, precision: 12, scale: 2, default: 0.0
    add_column :PlayerStats_Golf, :GreensPercent, :decimal, precision: 12, scale: 2, default: 0.0
    add_column :PlayerStats_Golf, :AverageDrive, :decimal, precision: 12, scale: 2, default: 0.0
    add_column :PlayerStats_Golf, :LongestDrive, :integer, default: 0

    add_column :TeamStats_Baseball, :AtBats, :tinyint, default: 0
    add_column :TeamStats_Baseball, :RBI, :tinyint, default: 0
    add_column :TeamStats_Baseball, :Walks, :tinyint, default: 0
    add_column :TeamStats_Baseball, :Strikeouts, :tinyint, default: 0
    add_column :TeamStats_Baseball, :Pitches, :int, default: 0

    add_column :Players, :ImageURL, :string, limit:150
    add_column :Players, :Position, :string, limit:50
    add_column :Teams, :ImageURL, :string, limit:150

    change_column :PlayerStats_Basketball, :FGPercent, :decimal, precision: 12, scale: 2, default: 0.0
    change_column :PlayerStats_Basketball, :ThreePtPercent, :decimal, precision: 12, scale: 2, default: 0.0
    change_column :PlayerStats_Basketball, :FTPercent, :decimal, precision: 12, scale: 2, default: 0.0

    change_column :PlayerStats_Baseball, :PitchingInnings, :decimal, precision: 12, scale: 1, default: 0.0

    change_column :PlayerStats_Football, :PassingRating, :decimal, precision: 12, scale: 1, default: 0.0
    change_column :PlayerStats_Football, :PassingCompletionsPct, :decimal, precision: 12, scale: 2, default: 0.0

    change_column :TeamStats_Hockey, :PPPercent, :decimal, precision: 12, scale: 2, default: 0.0
    change_column :TeamStats_Hockey, :FaceoffPercent, :decimal, precision: 12, scale: 2, default: 0.0

    remove_column :TeamStats_Hockey, :Shots_1, :integer, default: 0
    remove_column :TeamStats_Hockey, :Shots_2, :integer, default: 0
    remove_column :TeamStats_Hockey, :Shots_3, :integer, default: 0
  end
end