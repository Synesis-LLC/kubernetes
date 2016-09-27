/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//
// utility functions to setup rbd volume
// mainly implement diskManager interface
//

package rbd

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/util/node"
	"k8s.io/kubernetes/pkg/volume"
)


const (
	imageWatcherStr = "watcher="
)

// search /sys/bus for rbd device that matches given pool and image
func isPathExist(path string) (bool) {
	if _, err := os.Lstat(path); err == nil {
		return true
	}

	return false
}

// stat a path, if not exists, retry maxRetries times
func waitForPath(path string, maxRetries int) (bool) {
	for i := 0; i < maxRetries; i++ {
		found := isPathExist(path)
		if found {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

// make a directory like /var/lib/kubelet/plugins/kubernetes.io/pod/rbd/pool-image-image
func makePDNameInternal(host volume.VolumeHost, pool string, image string) string {
	return path.Join(host.GetPluginDir(rbdPluginName), "rbd", pool+"-image-"+image)
}

type RBDUtil struct{}

func (util *RBDUtil) MakeGlobalPDName(rbd rbd) string {
	return makePDNameInternal(rbd.plugin.host, rbd.Pool, rbd.Image)
}

type ImageWatcher struct {
	Address string `xml:"address"`
	Client  string `xml:"client"`
	Cookie  uint64 `xml:"cookie"`
}

type LockInfo struct {
	Name string
	Locker string
}

func logCmdInvoke(cmd string, args []string, output string) {
	glog.Infof("CMD: %s %s. Output: %s", cmd, strings.Join(args, " "), output)
}

func getOwnIPList() map[string]bool {
	ownIPList := map[string]bool{}

	ifaces, err := net.Interfaces()

	if (err != nil) {
		return ownIPList
	}

	for _, iface := range ifaces {
		addrs, _ := iface.Addrs()

		for _, addr := range addrs {
			ownIPList[strings.Split(addr.String(), "/")[0]] = true
		}
	}

	return ownIPList
}

func createCephConfFile(secret string, monitors []string) (string, error) {
	f, err := ioutil.TempFile("", "ceph.conf")

	if err != nil {
		return "", err
	}

	defer f.Close()
	defer f.Sync()

	monitorsString := strings.Join(monitors, ",")

	f.WriteString(fmt.Sprintf(`
[global]
mon host = %s
mon addr = %s
key = %s
`, monitorsString, monitorsString, secret))

	return f.Name(), err
}

func removeFromBlacklist(address string, mounter rbdMounter, cephConfFileName string) error {
	args := []string{"osd", "blacklist", "rm", address, "-c=" + cephConfFileName}

	cmd, err := mounter.plugin.execCommand("ceph", args)
	output := string(cmd)

	logCmdInvoke("ceph", args, output)

	if err != nil {
		glog.Error("Failed to remove client from OSD blackilist.", output)
	}

	return err
}

func addToBlacklist(address string, mounter rbdMounter, cephConfFileName string) error {
	args := []string{"osd", "blacklist", "add", address, "-c=" + cephConfFileName}

	cmd, err := mounter.plugin.execCommand("ceph", args)
	output := string(cmd)

	logCmdInvoke("ceph", args, output)

	if err != nil {
		glog.Error("Failed to remove client from OSD blackilist.", output)
	}

	return err
}

func listLocks(mounter rbdMounter, cephConfFileName string) (LockInfo, error) {
	args := []string{"lock", "list", mounter.Image, "--pool", mounter.Pool, "--id", mounter.Id, "--format=json",
		"-c=" + cephConfFileName}

	cmd, err := mounter.plugin.execCommand("rbd", args)
	output := string(cmd)

	// First lines may contain warning messages.
	outputLines := strings.Split(output, "\n")
	outputStr := outputLines[len(outputLines) - 1]

	logCmdInvoke("rbd", args, output)

	if err != nil {
		glog.Errorf("Failed to run rbd lock list. Error: %s", output)

		return LockInfo{}, err
	}

	var outputMap map[string] interface{}

	err = json.Unmarshal([]byte(outputStr), &outputMap)

	var outputLockInfo LockInfo

	for lockName, lockInfo := range outputMap {
		outputLockInfo.Name = lockName

		lockInfoAsMap := lockInfo.(map[string] interface{})

		outputLockInfo.Locker = lockInfoAsMap["locker"].(string)
	}

	glog.Infof("Lock info: %+v", outputLockInfo)

	return outputLockInfo, nil
}

func listImageWatchers(mounter rbdMounter, cephConfFileName string)  ([]ImageWatcher, error) {
	// We need to use XML output format instead of JSON due to invalid output of "rbd status" command.
	args := []string{"status", mounter.Image, "--pool", mounter.Pool, "--id", mounter.Id, "-c=" + cephConfFileName,
		"--format=xml"}

	cmd, err := mounter.plugin.execCommand("rbd", args)

	output := string(cmd)

	// First lines may contain warning messages.
	outputLines := strings.Split(output, "\n")
	outputStr := outputLines[len(outputLines) - 1]

	logCmdInvoke("rbd", args, outputStr)

	if (err != nil) {
		glog.Errorf("Failed to run rbd status command. Error: %s", output)
		return []ImageWatcher{}, err
	}

	type Result struct {
		XMLName  xml.Name `xml:"status"`
		Watchers []ImageWatcher `xml:"watchers>watcher"`
	}

	var outputNative Result

	err = xml.Unmarshal([]byte(outputStr), &outputNative)

	if (err != nil) {
		glog.Error("Failed to parse rbd status XML.")

		return []ImageWatcher{}, err
	}

	return outputNative.Watchers, nil
}

// remove a lock: rbd lock remove
func removeLock(mounter rbdMounter, lockInfo LockInfo, cephConfFileName string) error {
	if lockInfo.Name == "" {
		return nil
	}

	args := []string{"lock", "remove", mounter.Image, lockInfo.Name, lockInfo.Locker, "--pool", mounter.Pool,
			"--id", mounter.Id, "-c=" + cephConfFileName}

	glog.Infof("Remove RBD lock. Name: %s locker: %s", lockInfo.Name, lockInfo.Locker)

	cmd, err := mounter.plugin.execCommand("rbd", args)
	output := string(cmd)

	logCmdInvoke("rbd", args, output)

	if err != nil {
		glog.Errorf("Failed to remove RBD lock. Error: %s", output)
	}

	return err
}

// add a lock: rbd lock add
func addLock(mounter rbdMounter, lockID string, cephConfFileName string) error {
	args := []string{"lock", "add", mounter.Image, lockID, "--pool", mounter.Pool, "--id", mounter.Id,
		"-c=" + cephConfFileName}

	cmd, err := mounter.plugin.execCommand("rbd", args)
	output := string(cmd)

	logCmdInvoke("rbd", args, output)

	if err != nil {
		glog.Errorf("Failed to add RBD lock. Error: %s", output)
	}

	return err
}

func addWatchersToBlacklist(watchers []ImageWatcher, b rbdMounter, cephConfFileName string) error {
	ownIPList := getOwnIPList()

	for _, watcher := range watchers {
		address := watcher.Address
		ip := strings.Split(address, ":")[0]

		if _, exists := ownIPList[ip]; exists {
			fmt.Printf("Found own IP: %s. It will be removed from the OSD blacklist.\n", ip)
			err := removeFromBlacklist(address, b, cephConfFileName)

			if err != nil {
				return err
			}

		} else {
			fmt.Printf("Found foreign IP: %s. It will be added to the OSD blacklist.\n", ip)
			err := addToBlacklist(address, b, cephConfFileName)

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (util *RBDUtil) rbdLock(b rbdMounter, lock bool) error {
	// construct lock id using host name and a magic prefix
	lockID := "kubelet_lock_magic_" + node.GetHostname("")

	cephConfFileName, err := createCephConfFile(b.Secret, b.Mon)

	if err != nil {
		glog.Errorf("Failed to create ceph conf file. Error: %s", err.Error())

		return err
	}

	defer os.Remove(cephConfFileName)

	// cmd "rbd lock list" serves two purposes:
	// for fencing, check if lock already held for this host
	// this edge case happens if host crashes in the middle of acquiring lock and mounting rbd
	// for defencing, get the locker name, something like "client.1234"
	lockInfo, err := listLocks(b, cephConfFileName)

	if err != nil {
		return err
	}

	// check if lock is already held for this host by matching lock_id and rbd lock id
	if lockInfo.Name == lockID {
		// this host already holds the lock, exit
		if lock {
			glog.V(1).Infof("rbd: lock already held for %s", lockID)
			return nil
		}
	}

	if ! lock {
		if lockInfo.Name == lockID {
			return removeLock(b, lockInfo, cephConfFileName)
		}

		glog.Warning("Skipping unlocking RBD image with foreign lock")
		return nil
	}

	err = removeLock(b, lockInfo, cephConfFileName)

	if err != nil {
		return err
	}

	err = addLock(b, lockID, cephConfFileName)

	if err != nil {
		return err
	}

	watchers, err := listImageWatchers(b, cephConfFileName)

	if err != nil {
		glog.Errorf("Failed to list RBD image watchers. Error: %s", err.Error())

		return err
	}

	err = addWatchersToBlacklist(watchers, b, cephConfFileName)

	if err != nil {
		return err
	}

	// Make sure that RBD lock was acquired by us
	lockInfo, err = listLocks(b, cephConfFileName)

	if lockInfo.Name != lockID {
		return errors.New("Concurent RBD locking detected. Interrupt image mapping.")
	}

	return nil
}

func (util *RBDUtil) persistRBD(rbd rbdMounter, mnt string) error {
	file := path.Join(mnt, "rbd.json")
	fp, err := os.Create(file)
	if err != nil {
		return fmt.Errorf("rbd: create err %s/%s", file, err)
	}
	defer fp.Close()

	encoder := json.NewEncoder(fp)
	if err = encoder.Encode(rbd); err != nil {
		return fmt.Errorf("rbd: encode err: %v", err)
	}

	return nil
}

func (util *RBDUtil) loadRBD(mounter *rbdMounter, mnt string) error {
	file := path.Join(mnt, "rbd.json")
	fp, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("rbd: open err %s/%s", file, err)
	}
	defer fp.Close()

	decoder := json.NewDecoder(fp)
	if err = decoder.Decode(mounter); err != nil {
		return fmt.Errorf("rbd: decode err: %v", err)
	}

	return nil
}

func (util *RBDUtil) fencing(b rbdMounter) error {
	// no need to fence readOnly
	if (&b).GetAttributes().ReadOnly {
		return nil
	}
	return util.rbdLock(b, true)
}

func (util *RBDUtil) defencing(c rbdUnmounter) error {
	// no need to fence readOnly
	if c.ReadOnly {
		return nil
	}

	return util.rbdLock(*c.rbdMounter, false)
}

func (util *RBDUtil) AttachDisk(b rbdMounter) error {
	var err error
	var output []byte

	// modprobe
	_, err = b.plugin.execCommand("modprobe", []string{"rbd"})
	if err != nil {
		return fmt.Errorf("rbd: failed to modprobe rbd error:%v", err)
	}
	// rbd map
	l := len(b.Mon)
	// avoid mount storm, pick a host randomly
	start := rand.Int() % l
	// iterate all hosts until mount succeeds.

	for i := start; i < start+l; i++ {
		mon := b.Mon[i%l]
		glog.V(1).Infof("rbd: map mon %s", mon)

		mapArgs := []string{"map", b.Image, "--pool", b.Pool, "--id", b.Id, "-m", mon, "-o", "noshare"}

		if b.Secret != "" {
			mapArgs = append(mapArgs, "--key=" + b.Secret)
		} else {
			mapArgs = append(mapArgs, "-k", b.Keyring)
		}

		output, err = b.plugin.execCommand("rbd", mapArgs)
		logCmdInvoke("rbd", mapArgs, string(output))

		if err == nil {
			break
		}
		glog.V(1).Infof("rbd: map error %v %s", err, string(output))
	}
	if err != nil {
		return fmt.Errorf("rbd: map failed %v %s", err, string(output))
	}

	outputLines := strings.Split(strings.Trim(string(output), "\n "), "\n")
	devicePath := outputLines[len(outputLines) - 1]

	glog.Infof("Wait for mapped RBD device path: %s", devicePath)

	found := waitForPath(devicePath, 10)
	if !found {
		return errors.New("Could not map image: Timeout after 10s")
	}

	// mount it
	globalPDPath := b.manager.MakeGlobalPDName(*b.rbd)
	notMnt, err := b.mounter.IsLikelyNotMountPoint(globalPDPath)
	// in the first time, the path shouldn't exist and IsLikelyNotMountPoint is expected to get NotExist
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("rbd: %s failed to check mountpoint", globalPDPath)
	}
	if !notMnt {
		return nil
	}

	if err := os.MkdirAll(globalPDPath, 0750); err != nil {
		return fmt.Errorf("rbd: failed to mkdir %s, error", globalPDPath)
	}

	// fence off other mappers
	if err := util.fencing(b); err != nil {
		// rbd unmap before exit
		b.plugin.execCommand("rbd", []string{"unmap", devicePath})
		return fmt.Errorf("rbd: image %s is locked by other nodes", b.Image)
	}
	// rbd lock remove needs ceph and image config
	// but kubelet doesn't get them from apiserver during teardown
	// so persit rbd config so upon disk detach, rbd lock can be removed
	// since rbd json is persisted in the same local directory that is used as rbd mountpoint later,
	// the json file remains invisible during rbd mount and thus won't be removed accidentally.
	util.persistRBD(b, globalPDPath)

	if err = b.mounter.FormatAndMount(devicePath, globalPDPath, b.fsType, nil); err != nil {
		err = fmt.Errorf("rbd: failed to mount rbd volume %s [%s] to %s, error %v", devicePath, b.fsType, globalPDPath, err)
	}
	return err
}

func (util *RBDUtil) DetachDisk(c rbdUnmounter, mntPath string) error {
	device, cnt, err := mount.GetDeviceNameFromMount(c.mounter, mntPath)
	if err != nil {
		return fmt.Errorf("rbd detach disk: failed to get device from mnt: %s\nError: %v", mntPath, err)
	}
	if err = c.mounter.Unmount(mntPath); err != nil {
		return fmt.Errorf("rbd detach disk: failed to umount: %s\nError: %v", mntPath, err)
	}
	// if device is no longer used, see if can unmap
	if cnt <= 1 {
		// rbd unmap
		_, err = c.plugin.execCommand("rbd", []string{"unmap", device})
		if err != nil {
			return fmt.Errorf("rbd: failed to unmap device %s:Error: %v", device, err)
		}

		// load ceph and image/pool info to remove fencing
		if err := util.loadRBD(c.rbdMounter, mntPath); err == nil {
			// remove rbd lock
			util.defencing(c)
		}

		glog.Infof("rbd: successfully unmap device %s", device)
	}
	return nil
}

func (util *RBDUtil) CreateImage(p *rbdVolumeProvisioner) (r *api.RBDVolumeSource, size int, err error) {
	capacity := p.options.PVC.Spec.Resources.Requests[api.ResourceName(api.ResourceStorage)]
	volSizeBytes := capacity.Value()
	// convert to MB that rbd defaults on
	sz := int(volume.RoundUpSize(volSizeBytes, 1024*1024))
	volSz := fmt.Sprintf("%d", sz)
	// rbd create
	l := len(p.rbdMounter.Mon)
	// pick a mon randomly
	start := rand.Int() % l
	// iterate all monitors until create succeeds.
	for i := start; i < start+l; i++ {
		mon := p.Mon[i%l]
		glog.V(4).Infof("rbd: create %s size %s using mon %s, pool %s id %s key %s", p.rbdMounter.Image, volSz, mon, p.rbdMounter.Pool, p.rbdMounter.adminId, p.rbdMounter.adminSecret)
		var output []byte
		output, err = p.rbdMounter.plugin.execCommand("rbd",
			[]string{"create", p.rbdMounter.Image, "--size", volSz, "--pool", p.rbdMounter.Pool, "--id", p.rbdMounter.adminId, "-m", mon, "--key=" + p.rbdMounter.adminSecret, "--image-format", "1"})
		if err == nil {
			break
		} else {
			glog.Warningf("failed to create rbd image, output %v", string(output))
		}
	}

	if err != nil {
		glog.Errorf("rbd: Error creating rbd image: %v", err)
		return nil, 0, err
	}

	return &api.RBDVolumeSource{
		CephMonitors: p.rbdMounter.Mon,
		RBDImage:     p.rbdMounter.Image,
		RBDPool:      p.rbdMounter.Pool,
	}, sz, nil
}

func (util *RBDUtil) DeleteImage(p *rbdVolumeDeleter) error {
	var output []byte
	found, err := util.rbdStatus(p.rbdMounter)
	if err != nil {
		return err
	}
	if found {
		glog.Info("rbd is still being used ", p.rbdMounter.Image)
		return fmt.Errorf("rbd %s is still being used", p.rbdMounter.Image)
	}
	// rbd rm
	l := len(p.rbdMounter.Mon)
	// pick a mon randomly
	start := rand.Int() % l
	// iterate all monitors until rm succeeds.
	for i := start; i < start+l; i++ {
		mon := p.rbdMounter.Mon[i%l]
		glog.V(4).Infof("rbd: rm %s using mon %s, pool %s id %s key %s", p.rbdMounter.Image, mon, p.rbdMounter.Pool, p.rbdMounter.adminId, p.rbdMounter.adminSecret)
		output, err = p.plugin.execCommand("rbd",
			[]string{"rm", p.rbdMounter.Image, "--pool", p.rbdMounter.Pool, "--id", p.rbdMounter.adminId, "-m", mon, "--key=" + p.rbdMounter.adminSecret})
		if err == nil {
			return nil
		} else {
			glog.Errorf("failed to delete rbd image, error %v output %v", err, string(output))
		}
	}
	return err
}

// run rbd status command to check if there is watcher on the image
func (util *RBDUtil) rbdStatus(b *rbdMounter) (bool, error) {
	var err error
	var output string
	var cmd []byte

	l := len(b.Mon)
	start := rand.Int() % l
	// iterate all hosts until mount succeeds.
	for i := start; i < start+l; i++ {
		mon := b.Mon[i%l]
		// cmd "rbd status" list the rbd client watch with the following output:
		// Watchers:
		//   watcher=10.16.153.105:0/710245699 client.14163 cookie=1
		glog.V(4).Infof("rbd: status %s using mon %s, pool %s id %s key %s", b.Image, mon, b.Pool, b.adminId, b.adminSecret)
		cmd, err = b.plugin.execCommand("rbd",
			[]string{"status", b.Image, "--pool", b.Pool, "-m", mon, "--id", b.adminId, "--key=" + b.adminSecret})
		output = string(cmd)

		if err != nil {
			// ignore error code, just checkout output for watcher string
			glog.Warningf("failed to execute rbd status on mon %s", mon)
		}

		if strings.Contains(output, imageWatcherStr) {
			glog.V(4).Infof("rbd: watchers on %s: %s", b.Image, output)
			return true, nil
		} else {
			glog.Warningf("rbd: no watchers on %s", b.Image)
			return false, nil
		}
	}
	return false, nil
}
