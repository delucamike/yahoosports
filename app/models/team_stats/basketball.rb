# == Schema Information
#
# Table name: TeamStats_Basketball
#
#  GameID         :integer          not null
#  LeagueID       :integer          not null
#  TeamID         :integer          not null
#  Quarter_1      :integer          default(0)
#  Quarter_2      :integer          default(0)
#  Quarter_3      :integer          default(0)
#  Quarter_4      :integer          default(0)
#  Overtime_1     :integer          default(0)
#  Overtime_2     :integer          default(0)
#  Overtime_3     :integer          default(0)
#  Half_1         :integer          default(0)
#  Half_2         :integer          default(0)
#  FinalScore     :integer          default(0)
#  FGTaken        :integer          default(0)
#  FGMade         :integer          default(0)
#  FGPercent      :integer          default(0)
#  ThreePtTaken   :integer          default(0)
#  ThreePtMade    :integer          default(0)
#  ThreePtPercent :integer          default(0)
#  FTTaken        :integer          default(0)
#  FTMade         :integer          default(0)
#  FTPercent      :integer          default(0)
#  OffRebounds    :integer          default(0)
#  DefRebounds    :integer          default(0)
#  Assists        :integer          default(0)
#  Turnovers      :integer          default(0)
#  Steals         :integer          default(0)
#  Blocks         :integer          default(0)
#  BlocksAgainst  :integer          default(0)
#  PersonalFouls  :integer          default(0)
#  CreatedDate    :datetime
#  ModifiedDate   :datetime
#
# Indexes
#
#  GameID                                           (GameID)
#  LeagueID                                         (LeagueID)
#  TeamID                                           (TeamID)
#  index_TeamStats_Basketball_on_GameID_and_TeamID  (GameID,TeamID) UNIQUE
#

class TeamStats::Basketball < ActiveRecord::Base
  self.table_name = 'TeamStats_Basketball'
end
