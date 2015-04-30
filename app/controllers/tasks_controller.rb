class TasksController < ApplicationController
  before_action :set_task, only: [:show, :edit, :update, :destroy, :start, :stop]

  # GET /tasks
  def index
    @tasks = Task.all
  end

  # /tasks/new
  def new
    @task = Task.new
  end

  # POST /tasks
  def create
    @task = Task.new(task_params)

    if @task.save
      redirect_to tasks_url, notice: 'Task was created'
    else
      render :new
    end
  end

  # GET /tasks/1
  def edit
  end

  # PATCH/PUT /tasks/1
  def update
    if @task.update(task_params)
      redirect_to tasks_url, notice: 'Task was successfully updated'
    else
      render :edit
    end
  end

  # PUT /tasks/1/start
  def start
    @task.run!
    redirect_to tasks_url, notice: 'Task has started'
  end

  # PUT /tasks/1/start
  def stop
    @task.stop!
    redirect_to tasks_url, notice: 'Task has stopped'
  end

  def destroy
    @task.destroy
    redirect_to tasks_url, notice: 'Task was destroyed'
  end

  private
    # Use callbacks to share common setup or constraints between actions.
    def set_task
      @task = Task.find(params[:id])
    end

    # Only allow a trusted parameter "white list" through.
    def task_params
      params.require(:task).permit(:league_name, :interval)
    end
end
