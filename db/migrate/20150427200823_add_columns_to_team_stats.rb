class AddColumnsToTeamStats < ActiveRecord::Migration
  def change
     remove_column :TeamStats_Baseball, :Pitches, :integer, default: 0

     add_column :TeamStats_Baseball, :PitchingInnings, :decimal, precision: 12, scale: 2, default: 0.0
     add_column :TeamStats_Baseball, :PitchingHits, :tinyint, default: 0
     add_column :TeamStats_Baseball, :PitchingRuns, :tinyint, default: 0
     add_column :TeamStats_Baseball, :PitchingEarnedRuns, :tinyint, default: 0
     add_column :TeamStats_Baseball, :PitchingWalks, :tinyint, default: 0
     add_column :TeamStats_Baseball, :PitchingStrikeouts, :tinyint, default: 0
     add_column :TeamStats_Baseball, :PitchingHomeRuns, :tinyint, default: 0

     change_column :PlayerStats_Basketball, :Points, :tinyint, default: 0
     change_column :PlayerStats_Basketball, :Minutes, :tinyint, default: 0
  end
end
