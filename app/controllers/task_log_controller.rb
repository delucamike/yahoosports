class TaskLogController < ApplicationController
  before_action :set_task, only: [:index]

  # GET /tasks/1/task_log
  def index
  end

  private

    def set_task
      @task = Task.find(params[:id])
    end
end
