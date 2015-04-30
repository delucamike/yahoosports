require 'rails_helper'

describe TaskRunnerService, type: :service do
  let(:task_double) { double('Task') }
  let(:service) { TaskRunnerService.new(task_double) }

  before(:each) { Timecop.freeze('29/04/2015') }

  describe '#prepare_scrape_dates' do
    context 'given the SCRAPE_DATE environment variable' do
      context 'SCRAPE_DATE is nil' do
        it 'returns an array with today and yesterday dates' do
          today_date = Time.zone.now

          result = service.prepare_scrape_dates
          expect(result).to eq([today_date, today_date.yesterday])
        end
      end

      context 'SCRAPE_DATE is filled in' do
        it 'returns an array with the date and day before the given SCRAPE_DATE' do
          ENV['SCRAPE_DATE'] = '10/04/2015'
          given_date = Time.zone.parse('10/04/2015')

          result = service.prepare_scrape_dates
          expect(result).to eq([given_date, given_date.yesterday])
        end
      end
    end
  end
end