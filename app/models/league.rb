# == Schema Information
#
# Table name: Leagues
#
#  LeagueID   :integer          not null, primary key
#  LeagueName :string(50)
#  SportID    :integer
#  SportName  :string(50)
#

class League < ActiveRecord::Base
  self.table_name = 'Leagues'
  self.primary_key = 'LeagueID'
end
