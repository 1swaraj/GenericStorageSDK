import React, { Component } from "react";

export default class Login extends Component {
  constructor(props,context) {
    super(props,context);
    this.state = { bucket: '' , object: '',cloud: 's3'};
    this.onSubmit = this.onSubmit.bind(this);

  }
    onSubmit(e) {
        var saveAs = require('file-saver');
        e.preventDefault();
        let payload = "download,"+this.refs["cloud"].value+"://"+this.refs["bucket"].value+","+this.refs["object"].value
        var requestOptions = {
            method: 'POST',
            body: payload,
            responseType: 'blob',
            };

        fetch(`https://uoo8qa9te4.execute-api.ap-southeast-1.amazonaws.com/v0`, requestOptions)
            .then(response => response.blob())
            .then(blob => saveAs(blob, this.refs["object"].value))
            .catch((err) => {
            return Promise.reject({ Error: 'Something Went Wrong', err });
        })

    }



    render() {
        return (
            <form   action={this.props.action}
                    method={this.props.method}
                    onSubmit={this.onSubmit}>

                <h3>Download File</h3>

                <div className="form-group">
                    <label>Bucket Name</label>
                    <input type="text" ref="bucket" className="form-control" placeholder="Enter Bucket Name" />
                </div>
                
                <div className="form-group">
                    <label>Object Name</label>
                    <input type="text" ref="object" className="form-control" placeholder="Enter Object Name" />
                </div>
                
                <div className="form-group">
                    <select ref="cloud">
                        <option value="s3">AWS S3</option> 
                        <option value="gs">GCP Google Storage</option>
                    </select>
                </div>    

                <input type="submit" value="submit" className="btn btn-dark btn-lg btn-block"/>
            </form>
        );
    }
}
