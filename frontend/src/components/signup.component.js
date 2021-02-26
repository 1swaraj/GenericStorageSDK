import React, { Component } from "react";

export default class SignUp extends Component {
    constructor(props,context) {
    super(props,context);
    this.state = { bucket: '' , object: '',cloud: 's3'};
    this.onSubmit = this.onSubmit.bind(this);

  }
    onSubmit(e) {
        e.preventDefault();
        var input = document.querySelector('input[type="file"]')

        var formdata = new FormData();
        formdata.append("bucket", this.refs["cloud"].value+"://"+this.refs["bucket"].value);
        formdata.append(this.refs["object"].value, input.files[0]);

        var requestOptions = {
          method: 'POST',
          body: formdata,
          redirect: 'follow'
        };

        fetch("https://uoo8qa9te4.execute-api.ap-southeast-1.amazonaws.com/v0", requestOptions)
          .then(response => response.text())
          .then(result => {alert(result);console.log(result);})
          .catch(error => console.log('error', error));

    }



    render() {
        return (
            <form   action={this.props.action}
                    method={this.props.method}
                    onSubmit={this.onSubmit}>

                <h3>Upload File</h3>

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

                <div className="form-group">
                      <input type="file" ref="file"/>
                </div>

                <input type="submit" value="submit" className="btn btn-dark btn-lg btn-block"/>
            </form>
        );
    }
}