use dslab_scheduling::monitoring::ResourceLoad;

#[test]
fn test_monitoring() {
    let mut load = ResourceLoad::new_fraction(0., 100.0, Some(10.0));
    load.update(50., 5.);
    load.update(0., 11.);

    println!("{:?}", load.dump())
}
