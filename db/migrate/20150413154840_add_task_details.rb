class AddTaskDetails < ActiveRecord::Migration
  def change
    add_column :tasks, :start_time, :datetime
    add_column :tasks, :end_time, :datetime
    add_column :tasks, :runs, :integer, default: 0
    add_column :tasks, :records_updated, :integer, default: 0
    add_column :tasks, :records_inserted, :integer, default: 0
  end
end
