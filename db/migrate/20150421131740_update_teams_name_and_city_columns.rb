class UpdateTeamsNameAndCityColumns < ActiveRecord::Migration
  def up
    remove_column 'Teams', 'TeamCity', :string
    add_column    'Teams', 'TeamFullName', :string
  end

  def down
    add_column    'Teams', 'TeamCity', :string
    remove_column 'Teams', 'TeamFullName', :string
  end
end
