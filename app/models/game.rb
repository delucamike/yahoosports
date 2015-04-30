# == Schema Information
#
# Table name: Games
#
#  GameID       :integer          not null, primary key
#  LeagueID     :integer          not null
#  GameTitle    :string(150)
#  HomeTeamID   :integer
#  AwayTeamID   :integer
#  Attendance   :integer          default(0)
#  StartDate    :datetime
#  InProgress   :integer          default(0)
#  ESPNUrl      :string(150)      not null
#  CreatedDate  :datetime         not null
#  ModifiedDate :datetime         not null
#
# Indexes
#
#  index_matches_on_away_team_id  (AwayTeamID)
#  index_matches_on_home_team_id  (HomeTeamID)
#

class Game < ActiveRecord::Base
  self.table_name = 'Games'
  self.primary_key = 'GameID'
end
