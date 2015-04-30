class RemoveTaskDetails < ActiveRecord::Migration
  def change
    remove_column :tasks, :start_time, :datetime
    remove_column :tasks, :end_time, :datetime
    remove_column :tasks, :runs, :integer, default: 0
    remove_column :tasks, :records_updated, :integer, default: 0
    remove_column :tasks, :records_inserted, :integer, default: 0
  end
end
