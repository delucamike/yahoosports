# == Schema Information
#
# Table name: task_logs
#
#  id                :integer          not null, primary key
#  task_id           :integer
#  start_time        :datetime
#  end_time          :datetime
#  records_updated   :integer          default(0)
#  records_inserted  :integer          default(0)
#  games_in_progress :integer          default(0)
#  league_name       :string(255)
#  created_at        :datetime         not null
#  updated_at        :datetime         not null
#
# Indexes
#
#  index_task_logs_on_task_id  (task_id)
#

require 'rails_helper'

RSpec.describe TaskLog, type: :model do
  it { is_expected.to belong_to(:task) }

  it { is_expected.to validate_presence_of(:task) }

  describe 'after saving' do
    describe '.keep_max_logs' do
      let(:task) { create(:task) }
      let(:another_task) { create(:task) }

      it 'deletes old logs, keeping track of the last MAX_LOGS_NUMBER of Task Logs' do
        stub_const('TaskLog::MAX_LOGS_NUMBER', 2)
        create_list(:task_log, 3, task: task)
        expect(TaskLog.count).to eq(2)
      end

      it 'only deletes tasks from the TaskLog task' do
        stub_const('TaskLog::MAX_LOGS_NUMBER', 1)

        task_log = create(:task_log, task: task)
        another_task_log = create(:task_log, task: another_task)

        last_another_task_log = create(:task_log, task: another_task)

        expect(task.logs).to match_array([task_log])
        expect(another_task.logs).to match_array([last_another_task_log])
      end
    end
  end
end
