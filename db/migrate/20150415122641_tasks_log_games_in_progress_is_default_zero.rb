class TasksLogGamesInProgressIsDefaultZero < ActiveRecord::Migration
  def up
    change_column :task_logs, :games_in_progress, :integer, default: 0
  end

  def down
    change_column :task_logs, :games_in_progress, :integer
  end
end
