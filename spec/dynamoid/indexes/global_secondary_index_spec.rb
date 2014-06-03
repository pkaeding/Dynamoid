require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

describe "Dynamoid::Indexes::GlobalSecondaryIndex" do
  
  before do
    @time = DateTime.now
    @index = Dynamoid::Indexes::GlobalSecondaryIndex.new(User, :name, :range_key => :created_at, :projection => [ :password ])
  end

  it 'reorders keys alphabetically' do
    @index.sort([:name, :created_at]).should == [:created_at, :name]
  end
  
  it 'assigns itself hash keys' do
    @index.hash_key.should == :name
  end
  
  it 'assigns itself range keys' do
    @index.range_key.should == :created_at
  end
  
  it 'reorders its name automatically' do
    @index.name.should == [:created_at, :name]
  end
  
  it 'determines its own table name' do
    @index.index_name.should == 'index_created_ats_and_names'
  end
  
  it 'raises an error if a field does not exist' do
    lambda {@index = Dynamoid::Indexes::GlobalSecondaryIndex.new(User, [:password, :text])}.should raise_error(Dynamoid::Errors::InvalidField)
  end
  
  it 'describes itself properly to allow inedx creation' do
    @index.to_hash == {
          :index_name => 'index_created_ats_and_names',
          :key_schema => [
                            {
                              :attribute_name => :name,
                              :key_type => 'HASH'
                            },
                            {
                              :attribute_name => :created_at,
                              :key_type => 'RANGE'
                            }
                          ],
          :projection => { 
                            :projection_type => 'INCLUDE', 
                            :non_key_attributes => [ :password ]
                          },
          :provisioned_throughput => {
            :read_capacity_units => 50,
            :write_capacity_units => 50
          }
        }
  end
end