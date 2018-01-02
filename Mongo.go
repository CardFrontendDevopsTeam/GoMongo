/*
Package database provides an easy mechanism to allow an application to create a <ongo DB Connection as the application
starts up.

The package looks for Environment Parameters and allows for either a connection string or individual elements.

First, it looks to see if the MONGO environment variable is set. The MONGO Environment variable, if set, should contain
a mongo conneciton string, for example
mongodb://db1.example.net:27017,db2.example.net:2500/?replicaSet=test

If the MONGO environemnt variable is not set, the code moves onto the individual environment variables listed below
* MONGO_SERVERS - A comma seperates list of servers and port, for example db1.example.net:27017,db2.example.net:2500
* MONGO_USER - The username
* MONGO_PASSWORD - The password
* MONGO_DATABASE - The Database
* MONGO_REPLICA_SET - Replica set name
* MONGO_AUTH_SOURCE - Auth source
* MONGO_SSL - Boolean indicate SSL

 */
package database

import (
	"gopkg.in/mgo.v2"
	"log"
	"net"
	"crypto/tls"
	"net/url"
	"strings"
	"time"
	"strconv"
	"errors"
)

var Database *mgo.Database

func init() {
	log.Println("Starting Database")

	mongo :=mongoConnectionString()

	var dialinfo *mgo.DialInfo

	if mongo == "" {
		dialinfo = getDialInfoParameters()
	} else {
		var err error
		dialinfo, err = parseMongoURL(mongo)
		if err != nil {
			log.Fatal(err)
		}
	}
	session, err := mgo.DialWithInfo(dialinfo)
	if err != nil {
		log.Panic(err)
	}
	session.SetMode(mgo.Monotonic, true)

	Database = session.DB(dialinfo.Database)

}

func getDialInfoParameters() *mgo.DialInfo{
	dialinfo := mgo.DialInfo{}
	dialinfo.Addrs = mongoServers()
	dialinfo.Database = mongoDB()
	dialinfo.Password = mongoPassword()
	dialinfo.Username = mongoUser()
	dialinfo.ReplicaSetName = mongoReplicaSet()
	dialinfo.Source = mongoAuthSource()

	ssl := mongoSSL()

	if ssl {
		dialinfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), &tls.Config{})
		}
	}
	return &dialinfo
}

func parseMongoURL(rawURL string) (*mgo.DialInfo, error) {
	url, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	info := mgo.DialInfo{
		Addrs:    strings.Split(url.Host, ","),
		Database: strings.TrimPrefix(url.Path, "/"),
		Timeout:  10 * time.Second,
	}

	if url.User != nil {
		info.Username = url.User.Username()
		info.Password, _ = url.User.Password()
	}

	query := url.Query()
	for key, values := range query {
		var value string
		if len(values) > 0 {
			value = values[0]
		}

		switch key {
		case "authSource":
			info.Source = value
		case "authMechanism":
			info.Mechanism = value
		case "gssapiServiceName":
			info.Service = value
		case "replicaSet":
			info.ReplicaSetName = value
		case "maxPoolSize":
			poolLimit, err := strconv.Atoi(value)
			if err != nil {
				return nil, errors.New("bad value for maxPoolSize: " + value)
			}
			info.PoolLimit = poolLimit
		case "ssl":
			// Unfortunately, mgo doesn't support the ssl parameter in its MongoDB URI parsing logic, so we have to handle that
			// ourselves. See https://github.com/go-mgo/mgo/issues/84
			ssl, err := strconv.ParseBool(value)
			if err != nil {
				return nil, errors.New("bad value for ssl: " + value)
			}
			if ssl {
				info.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
					return tls.Dial("tcp", addr.String(), &tls.Config{})
				}
			}
		case "connect":
			if value == "direct" {
				info.Direct = true
				break
			}
			if value == "replicaSet" {
				break
			}
			fallthrough
		default:
			return nil, errors.New("unsupported connection URL option: " + key + "=" + value)
		}
	}

	return &info, nil
}
