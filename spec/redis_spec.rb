require File.dirname(__FILE__) + '/spec_helper'
require 'redis/namespace'
require 'logger'

describe "redis" do
  before(:all) do
    # use database 15 for testing so we dont accidentally step on you real data
    @redis = Redis.new :db => 15
    @namespaced = Redis::Namespace.new(:ns, :redis => @redis)
  end

  before(:each) do
    @namespaced.flushdb
    @redis['foo'] = 'bar'
  end

  after(:each) do
    @redis.flushdb
  end

  after(:all) do
    @redis.quit
  end

  it "should be able to use a namespace" do
    @namespaced['foo'].should == nil
    @namespaced['foo'] = 'chris'
    @namespaced['foo'].should == 'chris'
    @redis['foo'] = 'bob'
    @redis['foo'].should == 'bob'

    @namespaced.incr('counter', 2)
    @namespaced['counter'].to_i.should == 2
    @redis['counter'].should == nil
    @namespaced.type('counter').should == 'string'
  end

  it "should be able to use a namespace with mget" do
    @namespaced['foo'] = 1000
    @namespaced['bar'] = 2000
    @namespaced.mapped_mget('foo', 'bar').should == { 'foo' => '1000', 'bar' => '2000' }
    @namespaced.mapped_mget('foo', 'baz', 'bar').should == {'foo'=>'1000', 'bar'=>'2000'}
  end

  it "should be able to use a namespace with mset" do
    @namespaced.mset('foo' => '1000', 'bar' => '2000')
    @namespaced.mapped_mget('foo', 'bar').should == { 'foo' => '1000', 'bar' => '2000' }
    @namespaced.mapped_mget('foo', 'baz', 'bar').should == { 'foo' => '1000', 'bar' => '2000'}
  end

  it "should be able to use a namespace with msetnx" do
    @namespaced.msetnx('foo' => '1000', 'bar' => '2000')
    @namespaced.mapped_mget('foo', 'bar').should == { 'foo' => '1000', 'bar' => '2000' }
    @namespaced.mapped_mget('foo', 'baz', 'bar').should == { 'foo' => '1000', 'bar' => '2000'}
  end

  it "should be able to use a namespace in a pipeline" do
    @namespaced.pipelined do |namespaced_pipeline|
      namespaced_pipeline['foo'] = 'chris'
      namespaced_pipeline['bar'] = 'filip'
      namespaced_pipeline.instance_variable_get(:@commands).length.should == 2
    end

    @namespaced['foo'].should == 'chris'
    @namespaced['bar'].should == 'filip'
  end

  it "should not be able to use a namespace in a pipeline for mget command" do
    lambda {
      @namespaced.pipelined do |namespaced_pipeline|
        namespaced_pipeline.mget('foo', 'bar')
      end
    }.should raise_error(RuntimeError)
  end

  it "should not be able to use a namespace in a pipeline for mset command" do
    lambda {
      @namespaced.pipelined do |namespaced_pipeline|
        namespaced_pipeline.mset('foo' => '1000', 'bar' => '2000')
      end
    }.should raise_error(RuntimeError)
  end

  it "should not be able to use a namespace in a pipeline for msetnx command" do
    lambda {
      @namespaced.pipelined do |namespaced_pipeline|
        namespaced_pipeline.msetnx('foo' => '1000', 'bar' => '2000')
      end
    }.should raise_error(RuntimeError)
  end
end
