/**
 * Copyright (c) 2013, ideawu
 * All rights reserved.
 * @author: ideawu
 * @link: http://www.ideawu.com/
 *
 * SSDB nodejs client SDK.
 */

var net = require('net');

// timeout: microseconds, if ommitted, it will be treated as listener
// callback(err, ssdb)
exports.connect = function(host, port, timeout, listener){
	var self = this;
	var recv_buf = Buffer.alloc(0);
	var callbacks = [];
	var connected = false;

	if(typeof(timeout) == 'function'){
		listener = timeout;
		timeout = 0;
	}
	timeout = timeout || 0;
	listener = listener || function(){};

	var sock = new net.Socket();
	sock.on('error', function(e){		
		console.log('333');
		if(!connected){
			listener('connect_failed', e);
		}else{
			var callback = callbacks.shift();
			callback(['error']);
		}
	});
	
	sock.on('data', function(data){
		recv_buf = build_buffer([recv_buf, data]);
		while(recv_buf.length > 0){
			var resp = parse();
			if(!resp){
				break;
			}
			resp[0] = resp[0].toString();
			var callback = callbacks.shift();
			callback(resp);
		}
	});

	sock.connect({port:8888, host:'localhost'}, function(){
		//console.log('Socket connected!');		
		connected = true;
		sock.setNoDelay(true);
		sock.setKeepAlive(true);
		sock.setTimeout(0); //timeout);
		listener(0, self);
	});

	self.close = function(){
		sock.end();
	}

	self.request = function(cmd, params, callback){
		callbacks.push(callback || function(){});
		var arr = [cmd].concat(params);
		self.send_request(arr);
	}

	function build_buffer(arr){
		var bs = [];
		var size = 0;
		for(var i = 0; i < arr.length; i++){
			var arg = arr[i];
			if(arg instanceof Buffer){
				//
			}else{
				arg = Buffer.from(arg.toString());
			}
			bs.push(arg);
			size += arg.length;
		}
		var ret = Buffer.alloc(size);
		var offset = 0;
		for(var i=0; i<bs.length; i++){
			bs[i].copy(ret, offset);
			offset += bs[i].length;
		}
		return ret;
	}

	self.send_request = function(params){
		var bs = [];
		for(var i=0;i<params.length;i++){
			var p = params[i];
			var len = 0;
			if(!(p instanceof Buffer)){
				p = p.toString();
				bs.push(Buffer.byteLength(p));
			}else{
				bs.push(p.length);
			}
			bs.push('\n');
			bs.push(p);
			bs.push('\n');
		}
		bs.push('\n');
		
		var req = build_buffer(bs);
		
		sock.write(req);
		//console.log('write ' + req.length + ' bytes');
		//console.log('write: ' + req);
	}

	function memchr(buf, ch, start){
		start = start || 0;
		ch = typeof(ch) == 'string'? ch.charCodeAt(0) : ch;
		for(var i=start; i<buf.length; i++){
			if(buf[i] == ch){
				return i;
			}
		}
		return -1;
	}

	function parse(){
		var ret = [];
		var spos = 0;
		var pos;
		//console.log('parse: ' + recv_buf.length + ' bytes');
		while(true){
			//pos = recv_buf.indexOf('\n', spos);
			pos = memchr(recv_buf, '\n', spos);
			if(pos == -1){
				// not finished
				return null;
			}
			var line = recv_buf.slice(spos, pos).toString();
			spos = pos + 1;
			line = line.replace(/^\s+(.*)\s+$/, '\1');
			if(line.length == 0){
				// parse end
				//recv_buf = recv_buf.substr(spos);
				recv_buf = recv_buf.slice(spos);
				break;
			}
			var len = parseInt(line);
			if(isNaN(len)){
				// error
				console.log('error 1');
				return null;
			}
			if(spos + len > recv_buf.length){
				// not finished
				//console.log(spos + len, recv_buf.length);
				//console.log('not finish');
				return null;
			}
			//var data = recv_buf.substr(spos, len);
			var data = recv_buf.slice(spos, spos + len);
			spos += len;
			ret.push(data);

			//pos = recv_buf.indexOf('\n', spos);
			pos = memchr(recv_buf, '\n', spos);
			if(pos == -1){
				// not finished
				console.log('error 3');
				return null;
			}
			// '\n', or '\r\n'
			//if(recv_buf.charAt(spos) != '\n' && recv_buf.charAt(spos) != '\r' && recv_buf.charAt(spos+1) != '\n'){
			var cr = '\r'.charCodeAt(0);
			var lf = '\n'.charCodeAt(0);
			if(recv_buf[spos] != lf && recv_buf[spos] != cr && recv_buf[spos+1] != lf){
				// error
				console.log('error 4 ' + recv_buf[spos]);
				return null;
			}
			spos = pos + 1;
		}
		return ret;
	}

	// callback(err, val);
	// err: 0 on sucess, or error_code(string) on error
	
	////////////////// Key Value
	
	//get key Get the value related to the specified key
	self.get = function(key, callback){
		self.request('get', [key], function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
				callback(err, resp[1].toString());
			}
		});
	}

	//set key value Set the value of the key.
	self.set = function(key, val, callback){
		self.request('set', [key, val], function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	//setx key value ttl Set the value of the key, with a time to live. 
    self.setx = function(key, val, ttl, callback){
		if (!ttl) ttl = 0;
			ttl = parseInt(ttl);		
		
        self.request('setx', [key, val, ttl], function(resp){
            if(callback){
                var err = resp[0] == 'ok'? 0 : resp[0];
                callback(err, resp[1].toString());
            }
        });
    }
	
	//setnx key value Set the string value in argument as value of the key only if the key doesn"t exist.
	self.setnx = function(key, val, callback){
        self.request('setnx', [key, val], function(resp){
            if(callback){
                var err = resp[0] == 'ok'? 0 : resp[0];
                callback(err, parseInt(resp[1].toString()));
            }
        });
    }
	
	//expire key ttl Set the time left to live in seconds, only for keys of KV type.
	self.expire = function(key, ttl, callback){
		if (!ttl) ttl = 0;
			ttl = parseInt(ttl);		
		
        self.request('expire', [key, ttl], function(resp){
            if(callback){
                var err = resp[0] == 'ok'? 0 : resp[0];
                callback(err, parseInt(resp[1].toString()));
            }
        });
    }
	
	//ttl key Returns the time left to live in seconds, only for keys of KV type.
    self.ttl = function(key, callback){
        self.request('ttl', [key], function(resp){
            if(callback){
                var err = resp[0] == 'ok'? 0 : resp[0];
                callback(err, resp[1].toString());
            }
        });
    }
	
	//getset key value Sets a value and returns the previous entry at that key.
	self.getset = function(key, val, callback){
        self.request('getset', [key, val], function(resp){
            if(callback){
                var err = resp[0] == 'ok'? 0 : resp[0];
                callback(err, resp[1].toString());
            }
        });
    }
	
	//del key Delete specified key.
	self.del = function(key, callback){
		self.request('del', [key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}
	
	//incr key [num] Increment the number stored at key by num.
	self.incr = function(key, num, callback){
        if (!num) num = 1;
		num = parseInt( num );		
		
		self.request('incr', [key, num], function(resp){
            if(callback){
                var err = resp[0] == 'ok'? 0 : resp[0];
                callback(err, parseInt( resp[1].toString() ));
            }
        });
    }
	
	//exists key Verify if the specified key exists.
	self.exists = function(key, callback){
		self.request('exists', [key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err, parseInt( resp[1].toString() ));
			}
		});
	}
	
	//bit operations @todo 
	
	
	//substr key start size Return part of a string.
	self.substr = function(key, start, size, callback){
        if (!start) start = 0;
		if (!size) size = 1;	
		
		self.request('substr', [key, start, size], function(resp){
            if(callback){
                var err = resp[0] == 'ok'? 0 : resp[0];
                callback(err, resp[1].toString());
            }
        });
    }
	
	//strlen key Return the number of bytes of a string.
	self.strlen = function(key, callback){
		self.request('strlen', [key], function(resp){
            if(callback){
                var err = resp[0] == 'ok'? 0 : resp[0];
                callback(err, parseInt( resp[1].toString() ));
            }
        });
    }
	
	//rkeys key_start key_end limit List keys in range (key_start, key_end], in reverse order.
	self.rkeys = function(key_start, key_end, limit, callback){
		if (typeof(limit) == 'function'){
			callback = limit;
			limit = 9223372036854775807;
		}
		
		self.request('rkeys', [key_start, key_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = [];
				for(var i=1; i<resp.length; i++){
					data.push( resp[i].toString() );
				}
				callback(err, data);
			}
		});
	}
	
	//rscan key_start key_end limit List key-value pairs with keys in range (key_start, key_end], in reverse order.
	self.rscan = function(key_start, key_end, limit, callback){
		if (typeof(limit) == 'function'){
			callback = limit;
			limit = 9223372036854775807;
		}
		
		self.request('rscan', [key_start, key_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length % 2 != 1){
					callback('error');
				}else{
					var data = {};
					for(var i=1; i<resp.length; i+=2){
						data[ resp[i].toString() ] = resp[i+1].toString();
					}
					callback(err, data);
				}
			}
		});
	}

	//multi_set key1 value1 key2 value2 ... Set multiple key-value pairs(kvs) in one method call.
	self.multi_set = function(kv, callback){
		let nkv = [];
		if (kv){		
			Object.keys(kv).forEach(function(k){
				if (kv[k]){
					nkv.push( k );
					nkv.push( kv[k] );
				}
			});
		}
				
		self.request('multi_set', nkv, function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
				callback(err, resp[1].toString());
			}
		});
	}

	//multi_get key1 key2 ... Get the values related to the specified multiple keys
	self.multi_get = function(k, callback){
		self.request('multi_get', k, function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
				var data = {};
				for(var i=1; i<resp.length; i+=2){
					data[ resp[i].toString() ] = resp[i+1].toString();
				}
				callback(err, data);
			}
		});
	}

	//multi_del key1 key2 ... Delete specified multiple keys.
	self.multi_del = function(k, callback){
		self.request('multi_del', k, function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
				callback(err, resp[1].toString());
			}
		});
	}

	//scan key_start key_end limit List key-value pairs with keys in range (key_start, key_end].
	self.scan = function(key_start, key_end, limit, callback){
		if (typeof(limit) == 'function'){
			callback = limit;
			limit = 9223372036854775807;
		}
		
		self.request('scan', [key_start, key_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length % 2 != 1){
					callback('error');
				}else{
					var data = {};
					for(var i=1; i<resp.length; i+=2){
						data[ resp[i].toString() ] = resp[i+1].toString();
					}
					callback(err, data);
				}
			}
		});
	}

	//keys key_start key_end limit List keys in range (key_start, key_end].
	self.keys = function(key_start, key_end, limit, callback){
		self.request('keys', [key_start, key_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = [];
				for(var i=1; i<resp.length; i++){
					data.push( resp[i].toString() );
				}
				callback(err, data);
			}
		});
	}

	//////////////////////////////////////////////

	// callback(score)
	self.zget = function(name, key, callback){
		self.request('zget', [name, key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length == 2){
					var score = parseInt(resp[1]);
					callback(err, score);
				}else{
					var score = 0;
					callback('error');
				}
			}
		});
	}

	// callback(size)
	self.zsize = function(name, callback){
		self.request('zsize', [name], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length == 2){
					var size = parseInt(resp[1]);
					callback(err, size);
				}else{
					var score = 0;
					callback('error');
				}
			}
		});
	}

	// callback(err);
	self.zset = function(name, key, score, callback){
		self.request('zset', [name, key, score], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	// callback(err);
	self.zdel = function(name, key, callback){
		self.request('zdel', [name, key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	// callback(err, {index:[], items:{key:score}})
	self.zscan = function(name, key_start, score_start, score_end, limit, callback){
		self.request('zscan', [name, key_start, score_start, score_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length % 2 != 1){
					callback('error');
				}else{
					var data = {};
					for(var i=1; i<resp.length; i+=2){
						data[ resp[i].toString() ] = parseInt(resp[i+1]);
					}
					callback(err, data);
				}
			}
		});
	}

	// callback(err, [])
	self.zlist = function(name_start, name_end, limit, callback){
		self.request('zlist', [name_start, name_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = [];
				for(var i=1; i<resp.length; i++){
					var k = resp[i].toString();
					data.push(k);
				}
				callback(err, data);
			}
		});
	}
	
	// callback(err,sum)
	self.zsum = function(name, score_start, score_end, callback){
		self.request('zsum', [name,score_start,score_end], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length == 2){
					var size = parseInt(resp[1]);
					callback(err, size);
				}else{
					callback('error');
				}
			}
		});
	}

	//////////////////////////////////////////////

	// callback(val)
	self.hget = function(name, key, callback){
		self.request('hget', [name, key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length == 2){
					callback(err, resp[1]);
				}else{
					callback('error');
				}
			}
		});
	}

	// callback(err);
	self.hset = function(name, key, val, callback){
		self.request('hset', [name, key, val], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	// callback(err);
	self.hdel = function(name, key, callback){
		self.request('hdel', [name, key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	// callback(err, {index:[], items:{key:score}})
	self.hscan = function(name, key_start, key_end, limit, callback){
		self.request('hscan', [name, key_start, key_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length % 2 != 1){
					callback('error');
				}else{
					var data{};
					for(var i=1; i<resp.length; i+=2){
						data[ resp[i].toString() ] = resp[i+1].toString();
					}
					callback(err, data);
				}
			}
		});
	}

	// callback(err, [])
	self.hlist = function(name_start, name_end, limit, callback){
		if (typeof(limit) == 'function'){
			callback = limit;
			limit = 9223372036854775807; //max elements at hash
		}
		self.request('hlist', [name_start, name_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = [];
				for(var i=1; i<resp.length; i++){

					data.push( resp[i].toString() );
				}
				callback(err, data);
			}
		});
	}

	// callback(size)
	self.hsize = function(name, callback){
		self.request('hsize', [name], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length == 2){
					var size = parseInt(resp[1]);
					callback(err, size);
				}else{
					var score = 0;
					callback('error');
				}
			}
		});
	}
	
	//hclear name Delete all keys in a hashmap.
	self.hclear = function(key, callback){
		self.request('hclear', [key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}
	
	//hgetall name Returns the whole hash, as an array of strings indexed by strings.	
	self.hgetall = function(key, callback){
		self.request('hgetall', [key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = {};
				for(var i=1; i<resp.length; i+=2){
					data[ resp[i].toString() ] = resp[i+1].toString();
				}
				callback(err, data);
			}
		});
	}
	
	//hexists name key Verify if the specified key exists in a hashmap.
	self.hexists = function(name, key, callback){
		self.request('hexists', [name, key], function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
				let res = parseInt( resp[1].toString() );
				
				if (res === 0) res = false; else res = true;
				
				callback(err, res);
			}
		});
	}
	
	//hincr name key [num] Increment the number stored at key in a hashmap by num
	self.hincr = function(name, key, num, callback){
		if (typeof(num) == 'function'){
			callback = num;
			num = 1;
		}
					
		self.request('hincr', [name, key, num], function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
		
				callback(err, parseInt( resp[1].toString() ));
			}
		});
	}
	
	//hrlist name_start name_end limit List hashmap names in range (name_start, name_end].
	self.hrlist = function(name_start, name_end, limit, callback){
		if (typeof(limit) == 'function'){
			callback = limit;
			limit = 9223372036854775807; //max elements at hash
		}
		
		self.request('hrlist', [name_start, name_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = [];
				for(var i=1; i<resp.length; i++){
					data.push( resp[i].toString() );
				}
				callback(err, data);
			}
		});
	}
	
	//hkeys name key_start key_end List keys of a hashmap in range (key_start, key_end].
	self.hkeys = function(name, key_start, key_end, limit, callback){
		if (typeof(limit) == 'function'){
			callback = limit;
			limit = 9223372036854775807; //max elements at hash
		}		
		
		self.request('hkeys', [name, key_start, key_end, limit], function(resp){			
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = [];
				for(var i=1; i<resp.length; i++){
					data.push( resp[i].toString() );
				}
				callback(err, data);
			}
		});
	}
	
	//hrscan name key_start key_end limit List key-value pairs with keys in range (key_start, key_end], in reverse order.
	self.hrscan = function(name, key_start, key_end, limit, callback){
		if (typeof(limit) == 'function'){
			callback = limit;
			limit = 9223372036854775807; //max elements at hash
		}
		
		self.request('hrscan', [name, key_start, key_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length % 2 != 1){
					callback('error');
				}else{
					var data = {};
					for(var i=1; i<resp.length; i+=2){
						data[ resp[i].toString() ] = resp[i+1].toString();
					}
					callback(err, data);
				}
			}
		});
	}
	
	//multi_hget name key1 key2 ... Get the values related to the specified multiple keys of a hashmap.
	self.multi_hget = function(name, keys, callback){
		if (name && keys && keys.length)
			keys.unshift( name );
				
		self.request('multi_hget', keys, function(resp){
	
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = {};
				for(var i=1; i<resp.length; i+=2){
					data[ resp[i].toString() ] = resp[i+1].toString();
				}
				callback(err, data);
			}
		});
	}
	
	//multi_hdel name key1 key2 ... Delete specified multiple keys in a hashmap.
	self.multi_hdel = function(name, keys, callback){
		if (name && keys && keys.length)
			keys.unshift( name );
				
		self.request('multi_hdel', keys, function(resp){
	
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];				
				callback(err);
			}
		});
	}
	
	//multi_hset name key1 value1 key2 value2 ... Set multiple key-value pairs(kvs) of a hashmap in one method call.
	self.multi_hset = function(name, kv, callback){
		let nkv = [];
		if (name && kv){
			nkv.push( name );
			
			Object.keys(kv).forEach(function(k){
				if (kv[k]){
					nkv.push( k );
					nkv.push( kv[k] );
				}
			});
		}
				
		self.request('multi_hset', nkv, function(resp){
	
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];				
				callback(err);
			}
		});
	}
	
	
	////////////////// Server command
	
	//auth password Authenticate the connection.
	self.auth = function(passw, callback){
		self.request('auth', [passw], function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
								
				callback(err);
			}
		});
	}
	
	//dbsize Return the approximate size of the database. In bytes
	self.dbsize = function(callback){	
		self.request('dbsize', [name, key, num], function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
		
				callback(err, parseInt( resp[1].toString() ));
			}
		});
	}
	
	//flushdb [type] Delete all data in ssdb server.
	self.flushdb = function(type, callback){	
		if (!type)	type = '';
		if (['', 'kv', 'hash', 'zset', 'list'].indexOf(type) == -1) type = '';
		
		self.request('flushdb', [type], function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
		
				callback(err);
			}
		});
	}
	
	//info [opt] Return the information of server.
	self.info = function(opt, callback){
		if (!opt) opt = '';
		self.request('info', [opt], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = [];
				
				for(var i=1; i<resp.length; i++){
					var k = resp[i].toString();
					data.push(k);
				}
				
				callback(err, data);
			}
		});
	}
	
	//slaveof id host port [auth last_seq last_key] Start a replication slave.
	self.slaveof = function(id, host, port, auth, last_seq, last_key, callback){	
		if (!last_seq)	last_seq = '';
		if (!last_key)	last_key = '';
		if (!auth)	auth = '';
		if (!port)	port = 8888;
				
		self.request('slaveof', [id, host, port, auth, last_seq, last_key], function(resp){
			if(callback){
				let err = resp[0] == 'ok'? 0 : resp[0];
		
				callback(err);
			}
		});
	}


	////////////////// IP Filter  @todo
	
	
	
	
	

	
	
	
	
	
	

	return self;
}


/*
example:
var SSDB = require('./SSDB.js');
var ssdb = SSDB.connect(host, port, function(err){
	if(err){
		return;
	}
	ssdb.set('a', new Date(), function(){
		console.log('set a');
	});
});
*/

